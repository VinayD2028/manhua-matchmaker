import streamlit as st
import sys
import os
import pickle
import pandas as pd

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.models.recommender import ManhuaRecommender
from src.app.utils import get_chapters, get_chapter_pages, fetch_image_bytes
import base64

# Page Config
st.set_page_config(
    page_title="Manhua Matchmaker",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .card {
        background-color: #1E1E1E;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border: 1px solid #333;
    }
    .score {
        color: #00FF00;
        font-weight: bold;
    }
    .reason {
        color: #888;
        font-style: italic;
        font-size: 0.9em;
    }
    /* Gapless Reader Fix */
    .reader-container img {
        display: block;
        margin-bottom: -1px;
    }
</style>
""", unsafe_allow_html=True)

# Helper for Base64
def get_image_as_base64(url):
    bytes_data = fetch_image_bytes(url)
    if bytes_data:
        return f"data:image/jpeg;base64,{base64.b64encode(bytes_data).decode()}"
    return None

# Application Logic
class App:
    def __init__(self):
        self.load_model()
    
    @st.cache_resource
    def load_model(_self):
        # We assume model is already trained via `recommender.py`
        rec = ManhuaRecommender(data_path="data/processed/merged_manhua.json")
        try:
            # Try loading existing model
            if not rec.load():
                 st.warning("Model not found. Training new model (this happens once)...")
                 rec.fit()
        except Exception as e:
            st.error(f"Error loading model: {e}")
            return None
        return rec

    def run(self):
        rec = self.load_model()
        if not rec: return
        
        # Sidebar
        st.sidebar.title("📚 Manhua Matchmaker")
        
        # Navigation State Handling (Manual Sync)
        if "page" not in st.session_state:
            st.session_state["page"] = "Discover"
            
        options = ["Discover", "Reader", "About"]
        try:
            # Get current index based on state
            curr_index = options.index(st.session_state["page"])
        except ValueError:
            curr_index = 0
            
        # Widget logic: No 'key' argument to avoid conflict. We sync manually.
        nav_selection = st.sidebar.radio("Navigation", options, index=curr_index)
        
        # Detect Sidebar Change
        if nav_selection != st.session_state["page"]:
            st.session_state["page"] = nav_selection
            st.rerun()
        
        if st.session_state["page"] == "Discover":
            self.render_discovery(rec)
        elif st.session_state["page"] == "Reader":
            self.render_reader()
        elif st.session_state["page"] == "About":
            self.render_about()

    def render_discovery(self, rec):
        st.title("Find Your Next Obsession")
        st.markdown("Describe the *exact* vibe you want. Example: *'Generic isekai but the MC is a skeleton'*")
        
        query = st.text_input("Search Query", placeholder="e.g. Cultivation manhua where he is evil")
        # Search Limit Slider
        num_results = st.sidebar.slider("Number of results to show", 10, 100, 20)
        
        if query:
            results = rec.recommend(query, top_k=num_results)
            
            st.write(f"Showing top {len(results)} matches for: **{query}**")
            for res in results:
                with st.container():
                    col1, col2 = st.columns([1, 4])
                    
                    with col1:
                        # Cover Image Logic - Use Proxy
                        if res.get('image'): 
                            cover_url = f"https://uploads.mangadex.org/covers/{res['id']}/{res['image']}.256.jpg"
                            cover_bytes = fetch_image_bytes(cover_url)
                            if cover_bytes:
                                st.image(cover_bytes, caption="Cover", use_container_width=True) 
                            else:
                                st.image("https://via.placeholder.com/150", caption="No Cover")
                        else:
                            st.image("https://via.placeholder.com/150", caption="No Cover")
                            
                    with col2:
                        st.markdown(f"### {res['title']} <span style='font-size:0.6em; color:grey'>({res.get('year', 'N/A')})</span>", unsafe_allow_html=True)
                        st.markdown(f"**Relevance:** `{res['score']:.2f}` | **{res['match_reason']}**")
                        
                        with st.expander("Why this match?"):
                            st.write(f"- 🧠 Vibe Similarity: `{res['dense_score']:.2f}`")
                            st.write(f"- 🔑 Keyword Match: `{res['sparse_score']:.2f}`")
                            if res.get('title_boost', 0) > 0:
                                st.write(f"- 🚀 Title Boost: `+{res['title_boost']:.2f}`")
                        
                        # Tags
                        st.caption(f"Tags: {', '.join(res['tags'][:5])}")
                        
                        # Description
                        with st.expander("Synopsis"):
                            st.write(res['description'])
                            
                        # Actions
                        if st.button(f"📖 Read {res['title']}", key=res['id']):
                            st.session_state['selected_manga'] = res['id']
                            st.session_state['selected_title'] = res['title']
                            st.session_state['current_chapter_index'] = 0 # Reset to Ch 1
                            st.session_state['page'] = "Reader" 
                            st.rerun() 
                            
                        if res.get('official_link'):
                            st.markdown(f"[Official Link]({res['official_link']})")

    def render_reader(self):
        st.title("📖 Ad-Free Reader")
        
        manga_id = st.session_state.get('selected_manga')
        manga_title = st.session_state.get('selected_title', "Unknown")
        
        if not manga_id:
            st.info("Select a Manhua from the 'Discover' tab first.")
            if st.button("Go to Overview"):
                st.session_state['page'] = "Discover"
                st.rerun()
            return

        st.subheader(f"Reading: {manga_title}")
        
        # 1. Fetch Chapters (Cached ideally, but calling utils for now)
        chapters = get_chapters(manga_id)
        if not chapters:
            st.error("No chapters found. (API Limit or Network Issue)")
            return
            
        # 2. State Management for Chapter Index
        if 'current_chapter_index' not in st.session_state:
            st.session_state['current_chapter_index'] = 0
            
        # Ensure index is within bounds (if we switched manga)
        if st.session_state['current_chapter_index'] >= len(chapters):
            st.session_state['current_chapter_index'] = 0

        current_idx = st.session_state['current_chapter_index']
        current_chap = chapters[current_idx]
        
        # Display Current Chapter Info
        # Create options list for dropdown
        chapter_options = [f"Ch {c['attributes']['chapter']} - {c['attributes']['title'] or ''}" for c in chapters]
        
        # --- Navigation Helper Function ---
        def render_nav_buttons(key_prefix):
            col1, col2, col3 = st.columns([1, 2, 1])
            
            # PREV Button
            with col1:
                if current_idx > 0:
                    if st.button("⬅️ Prev", key=f"{key_prefix}_prev", use_container_width=True):
                        st.session_state['current_chapter_index'] -= 1
                        st.rerun()
                else:
                    if st.button("⬅️ Overview", key=f"{key_prefix}_overview_prev", use_container_width=True):
                        st.session_state['page'] = "Discover"
                        st.rerun()
            
            # Center Dropdown & Refresh
            with col2:
                # We need a unique key for each selectbox (top/bottom)
                selected_opt = st.selectbox(
                    "Chapter Selection", 
                    options=chapter_options, 
                    index=current_idx, 
                    key=f"{key_prefix}_chap_select", 
                    label_visibility="collapsed"
                )
                
                # Check if user changed selection
                new_idx = chapter_options.index(selected_opt)
                if new_idx != current_idx:
                    st.session_state['current_chapter_index'] = new_idx
                    st.rerun()

                # Refresh Button (Only show on top nav to avoid clutter, or both?)
                # Adding it small below dropdown
                if key_prefix == "top":
                    if st.button("🔄 Refresh Images", key="refresh_chapter", help="Reload images if they failed to load"):
                        st.rerun()
            
            # NEXT Button
            with col3:
                if current_idx < len(chapters) - 1:
                    if st.button("Next ➡️", key=f"{key_prefix}_next", use_container_width=True):
                        st.session_state['current_chapter_index'] += 1
                        st.rerun()
                else:
                    if st.button("Overview ➡️", key=f"{key_prefix}_overview_next", use_container_width=True):
                        st.session_state['page'] = "Discover"
                        st.rerun()

        # 3. Render Top Navigation (Sticky Header)
        # We wrap this in a custom container to allow sticky positioning
        st.markdown('<div class="sticky-reader-header">', unsafe_allow_html=True)
        render_nav_buttons("top")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Inject CSS for the Sticky Header
        st.markdown("""
            <style>
            /* Target the specific div we just created */
            div:has(> .sticky-reader-header), 
            /* Fallback for older browsers or if 'has' is tricky in some Streamlit versions, 
               though generic div selection is hard. 
               Better approach: Target the direct parent of our marker class if possible, 
               but since Streamlit structure is complex, we style the marker itself 
               to fill spacing and use a fixed overlay for visual "stickiness" 
            */
            .sticky-reader-header {
                /* strictly for the marker div itself to be a container */
                width: 100%;
            }
            
            /* 
               Streamlit's vertical block layout makes pure "sticky" tricky 
               because the parent scroll context varies.
               However, 'position: fixed' works relative to viewport.
               But we want it to scroll naturally until it hits top? 
               The user asked for "header top center". sticky is best.
               
               We target the Streamlit Element container that wraps our HTML block + Buttons.
               Streamlit groups consecutive elements. 
               
               HACK: We will make a fixed Position overlay style for the controls.
               BUT standard Streamlit buttons cannot be easily moved out of their iframe flow 
               without breaking their functionality (sometimes).
               
               BETTER STRATEGY: 
               We simply style the container that "render_nav_buttons" lives in.
               Since we can't easily add a class to the st.columns container directly,
               we will rely on the fact that we wrapped it.
            */
            
            div[data-testid="stVerticalBlock"] > div:has(.sticky-reader-header) {
                position: sticky;
                top: 3rem; /* Height of Streamlit's main header usually */
                z-index: 999;
                background-color: #0E1117; /* Match theme background */
                padding-top: 10px;
                padding-bottom: 15px;
                border-bottom: 1px solid #262730;
                margin-top: -1rem; /* Pull it up slightly */
            }
            
            /* Adjust for when the Streamlit header is hidden or in different modes */
            @media (max-width: 640px) {
                div[data-testid="stVerticalBlock"] > div:has(.sticky-reader-header) {
                    top: 0rem;
                }
            }
            </style>
        """, unsafe_allow_html=True)
        
        # We use a set of timeouts to ensure it fires after the DOM update completes
        st.markdown("""
            <script>
                function scrollToTop() {
                    var body = window.parent.document.querySelector(".main");
                    if (body) body.scrollTop = 0;
                }
                setTimeout(scrollToTop, 50);
                setTimeout(scrollToTop, 200);
            </script>
            """, unsafe_allow_html=True)
            

        # 4. Auto-Load Pages (Gapless / Webtoon Mode)
        with st.spinner(f"Loading {chapter_options[current_idx]}..."):
            pages = get_chapter_pages(current_chap['id'])
            if pages:
                # Construct HTML with Base64 Images to bypass hotlinking
                html_content = """
                <div class="reader-container" style="
                    display: flex; 
                    flex-direction: column; 
                    align-items: center; 
                    width: 100%; 
                    margin: 0; 
                    padding: 0; 
                    background-color: #1E1E1E;
                ">
                """
                
                # We limit the number of pages processed at once if needed, 
                # but let's try all for now.
                for img_url in pages:
                    b64_img = get_image_as_base64(img_url)
                    if b64_img:
                        html_content += f'<img src="{b64_img}" style="display: block; width: auto; max-width: 100%; margin: 0; padding: 0; border: none; margin-bottom: -1px;" />'
                
                html_content += "</div>"
                st.markdown(html_content.replace('\n', ''), unsafe_allow_html=True)
            else:
                st.error("Could not load pages from MangaDex At-Home server.")
        
        # 5. Render Bottom Navigation
        st.divider()
        render_nav_buttons("bottom")

    def render_about(self):
        st.markdown("# About This Project")
        st.markdown("""
        **Manhua Matchmaker** is a Data Science Portfolio Project built to demonstrate:
        1.  **NLP & Vector Search**: Using `SentenceTransformers` and `FAISS` to understand semantic similarity.
        2.  **Hybrid Reranking**: Combining dense vectors with TF-IDF to solve the "Generic Isekai" problem.
        3.  **Data Engineering**: ETL pipeline merging data from MangaDex and Anilist.
        4.  **Full-Stack App**: Streamlit interface with a custom image proxy reader.
        
        *Built by [Your Name]*
        """)

if __name__ == "__main__":
    app = App()
    app.run()
