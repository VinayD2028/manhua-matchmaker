import json
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import faiss
import pickle
import os

class ManhuaRecommender:
    def __init__(self, data_path="data/processed/merged_manhua.json"):
        self.data_path = data_path
        self.output_index = "data/processed/manhua.index"
        self.output_model = "data/processed/recommender.pkl"
        
        # Load Data
        with open(data_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
        self.df = pd.DataFrame(self.data)
        
        # Handle missing descriptions
        self.df['description'] = self.df['description'].fillna("")
        
        # Initialize Models
        print("Loading SentenceTransformer model...")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.tfidf = TfidfVectorizer(stop_words='english')
        
        self.index = None
        self.tfidf_matrix = None
        self.embeddings = None

    def fit(self):
        print("Creating combined search text...")
        # Combine everything for a much stronger semantic signal
        self.df['combined_text'] = (
            self.df['title'] + " " + 
            self.df['title'] + " " + 
            self.df['alt_titles'].apply(lambda x: " ".join(x)) + " " + 
            self.df['tags'].apply(lambda x: " ".join(x)) + " " + 
            self.df['description']
        ).fillna("")

        print("Encoding combined text (optimized batch size)...")
        # 1. Generate Embeddings (Dense)
        self.embeddings = self.model.encode(
            self.df['combined_text'].tolist(), 
            show_progress_bar=True, 
            batch_size=128
        )
        self.embeddings = np.array(self.embeddings).astype('float32')
        
        # 2. Build FAISS Index
        print("Building FAISS index...")
        dimension = self.embeddings.shape[1]
        self.index = faiss.IndexFlatIP(dimension) 
        faiss.normalize_L2(self.embeddings)
        self.index.add(self.embeddings)
        
        # 3. TF-IDF
        print("Fitting TF-IDF model...")
        self.tfidf_matrix = self.tfidf.fit_transform(self.df['combined_text'])
        
        print("Model training complete.")
        self.save()

    def save(self):
        # Save FAISS index
        faiss.write_index(self.index, self.output_index)
        # Save TFIDF & Embeddings metadata
        with open(self.output_model, 'wb') as f:
            pickle.dump({
                'tfidf': self.tfidf,
                'tfidf_matrix': self.tfidf_matrix,
                'embeddings': self.embeddings
            }, f)

    def load(self):
        if not os.path.exists(self.output_index) or not os.path.exists(self.output_model):
            return False
            
        self.index = faiss.read_index(self.output_index)
        with open(self.output_model, 'rb') as f:
            data = pickle.load(f)
            self.tfidf = data['tfidf']
            self.tfidf_matrix = data['tfidf_matrix']
            self.embeddings = data['embeddings']
        return True

    def recommend(self, query, top_k=50):
        # 1. Dense Retrieval (Semantic Vibe)
        query_embedding = self.model.encode([query]).astype('float32')
        faiss.normalize_L2(query_embedding)
        
        # Retrieve top 200 candidates for reranking (increased from 50)
        k_candidates = min(200, len(self.df))
        if k_candidates == 0: return []
        
        D, I = self.index.search(query_embedding, k_candidates)
        
        candidates_idx = I[0]
        dense_scores = D[0]
        
        # 2. Sparse Reranking (Keyword Specificity)
        query_tfidf = self.tfidf.transform([query.lower()])
        query_words = set(query.lower().split())
        
        final_results = []
        
        for rank, idx in enumerate(candidates_idx):
            if idx == -1: continue
            
            item = self.df.iloc[idx].to_dict()
            title_lower = item['title'].lower()
            alt_titles_lower = " ".join([t.lower() for t in item.get('alt_titles', [])])
            
            # CALCULATE SCORES
            # Dense (Semantic)
            dense_score = dense_scores[rank]
            
            # Sparse (Keyword)
            sparse_score = cosine_similarity(query_tfidf, self.tfidf_matrix[idx])[0][0]
            
            # Title Matching Bonus
            title_boost = 0.0
            query_lower = query.lower()
            
            if query_lower in title_lower or query_lower in alt_titles_lower:
                title_boost = 0.5 # Huge boost for direct title matches
            elif any(word in title_lower.split() or word in alt_titles_lower.split() for word in query_words if len(word) > 3):
                title_boost = 0.2 # Significant boost for keyword matches in title
            
            # Final Hybrid Score
            final_score = (dense_score * 0.5) + (sparse_score * 0.3) + title_boost
            
            # Cap final score at 1.0
            final_score = min(float(final_score), 1.0)
            
            # DETERMINE MATCH REASON
            reason = "Matches plot vibe"
            if title_boost >= 0.5:
                reason = "🎯 Direct Title Match"
            elif title_boost > 0:
                reason = "⭐ Title Keyword Match"
            elif sparse_score > 0.4:
                reason = "🔍 Strong Keyword Match"
            
            final_results.append({
                "id": item['id'],
                "title": item['title'],
                "description": item['description'],
                "tags": item['tags'],
                "image": item.get('cover_art'), 
                "official_link": item.get('official_en_link'),
                "year": item.get('year'), 
                "score": final_score,
                "dense_score": float(dense_score),
                "sparse_score": float(sparse_score),
                "title_boost": title_boost,
                "match_reason": reason
            })
            
        # Sort by Final Hybrid Score
        final_results.sort(key=lambda x: x['score'], reverse=True)
        return final_results[:top_k]

    def _explain_match(self, query, item):
        # Improved heuristic with stop words removal
        from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
        
        query_words = set(query.lower().split())
        desc_words = set(item['description'].lower().split())
        
        # Filter stop words and common generic terms
        stop_words = set(list(ENGLISH_STOP_WORDS) + ['manhua', 'manga', 'comic', 'story', 'mc', 'main', 'character'])
        
        common = query_words.intersection(desc_words) - stop_words
        
        if common:
            return f"Matches keywords: {', '.join(list(common)[:4])}"
        return "Matches plot vibe"

if __name__ == "__main__":
    rec = ManhuaRecommender()
    rec.fit()
    
    # Test Query
    test_q = "system where he levels up"
    print(f"\nQuery: {test_q}")
    results = rec.recommend(test_q)
    for r in results[:3]:
        print(f"- {r['title']} (Score: {r['score']:.4f})")
