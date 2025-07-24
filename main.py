import streamlit as st
import os
import re
from agno.agent import Agent
from agno.models.google import Gemini
from dotenv import load_dotenv

def load_snippets():
    snippets = []
    snippet_dir = "snippets"
    for filename in sorted(os.listdir(snippet_dir)):
        if filename.endswith(".py"):
            with open(os.path.join(snippet_dir, filename), "r") as f:
                content = f.read()
                # Split snippets based on the separator
                parts = re.split(r'# --- snippet: (.*), tags: (.*) ---', content)
                if len(parts) > 1:
                    # The first part is usually an empty string or header before the first snippet
                    parsed_snippets = parts[0::3]
                    for i in range(1, len(parts), 3):
                        name = parts[i]
                        tags = [tag.strip() for tag in parts[i+1].split(',')]
                        code = parts[i+2].strip()
                        snippets.append({"name": name, "tags": tags, "code": code})
    return snippets

def main():
    st.set_page_config(layout="wide")
    
    load_dotenv()
    all_snippets = load_snippets()

    with st.sidebar:
        st.title("Spark Code Generator")

        st.header("APIs", divider="rainbow")
        rdd = st.checkbox("RDD")
        dataframe = st.checkbox("DataFrame / Spark SQL")

        st.header("Streaming", divider="rainbow")
        streaming_api = st.radio(
            "Streaming API",
            ("Batch", "DStream", "Structured Streaming"),
            label_visibility="collapsed"
        )

        st.header("Advanced", divider="rainbow")
        mllib = st.checkbox("MLlib")
        graphx = st.checkbox("GraphX (maps to GraphFrames in PySpark)")
        
        selected_tags = []
        if rdd:
            selected_tags.append("RDD")
        if dataframe:
            selected_tags.append("DataFrame / Spark SQL")
        
        selected_tags.append(streaming_api)

        if mllib:
            selected_tags.append("MLlib")
        if graphx:
            selected_tags.append("GraphX")

        available_snippets = [
            s for s in all_snippets 
            if any(tag in selected_tags for tag in s["tags"])
        ]
        
        tag_order = {
            "RDD": 0, "DataFrame / Spark SQL": 1,
            "Batch": 2, "DStream": 3, "Structured Streaming": 4,
            "MLlib": 5, "GraphX": 6
        }

        def get_snippet_priority(snippet):
            return min((tag_order.get(tag, 99) for tag in snippet["tags"]), default=99)

        available_snippets.sort(key=get_snippet_priority)

        st.header("Snippets", divider="rainbow")
        selected_snippets = []
        with st.container(border=True):
            for s in available_snippets:
                if st.checkbox(s["name"]):
                    selected_snippets.append(s["name"])

    final_code = "\n\n".join(
        s["code"] for s in available_snippets
        if s["name"] in selected_snippets
    )

    st.title("Test Data Platform - Pyspark generator")

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Input Code or Natural Language")
            input_code = st.text_area("Paste your code or describe what you want to build", height=400)
            if st.button("Convert", use_container_width=True):
                if input_code:
                    xml_tags = "".join([f"<api>{tag}</api>" for tag in selected_tags])
                    
                    snippet_objects = [s for s in available_snippets if s["name"] in selected_snippets]
                    reference_snippets_xml = "\n".join(
                        f'<snippet name="{s["name"]}"><![CDATA[\n{s["code"]}\n]]></snippet>'
                        for s in snippet_objects
                    )

                    prompt = f"""
<prompt>
    <role>
        You are an expert in Apache Spark. Your task is to generate PySpark code based on the user's request.
        The request may be in a natural language or a Java Spark code snippet to be converted.
    </role>
    <user_request>
        <![CDATA[
{input_code}
        ]]>
    </user_request>
    <instructions>
        <instruction>Generate idiomatic PySpark code that fulfills the user's request.</instruction>
        <instruction>Adhere to PySpark best practices.</instruction>
        <instruction>Use the provided context for guidance on required libraries and APIs.</instruction>
        <instruction>Your response must contain ONLY the generated PySpark code, enclosed within `<code>` tags.</instruction>
        <instruction>Do not include any explanations, comments, or markdown formatting outside of the `<code>` block.</instruction>
    </instructions>
    <context>
        <selected_apis>
            {xml_tags}
        </selected_apis>
        <reference_snippets>
{reference_snippets_xml}
        </reference_snippets>
    </context>
    <output_format>
        <code>
# Your PySpark code here
        </code>
    </output_format>
</prompt>
"""
                    with st.expander("View Full Prompt"):
                        st.code(prompt, language="xml")

                    with st.spinner("Generating PySpark code... 🤖"):
                        gemini_api_key = os.environ.get("GEMINI_API_KEY")
                        agent = Agent(model=Gemini(api_key=gemini_api_key))
                        response = agent.run(prompt)

                        code_block = response.content
                        # First, try to extract from <code> tags
                        match = re.search(r"<code>(.*?)</code>", code_block, re.DOTALL)
                        if match:
                            extracted_code = match.group(1).strip()
                        else:
                            # Fallback to using the whole response if <code> is missing
                            extracted_code = code_block.strip()

                        # Now, clean the extracted code using a more robust regex to remove
                        # markdown fences like ```python ... ``` that the model might add.
                        cleaned_code = re.sub(r"^(?:```|''')\s*python\n?", "", extracted_code)
                        cleaned_code = re.sub(r"\n?(?:```|''')$", "", cleaned_code).strip()
                        
                        st.session_state.output = cleaned_code

    with col2:
        with st.container(border=True):
            st.subheader("PySpark Code")
            st.code(st.session_state.get("output", final_code), language="python")

if __name__ == "__main__":
    main()
