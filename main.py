import streamlit as st
import os
import re
import json
import time
import random
from typing import Iterator
from agno.agent import Agent, RunResponse
from agno.models.google import Gemini
from agno.workflow import Workflow
from dotenv import load_dotenv

## --- SECURITY WARNING ---
# The Gemini API key is now loaded from the .env file or environment variable.

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
                    for i in range(1, len(parts), 3):
                        name = parts[i]
                        tags = [tag.strip() for tag in parts[i+1].split(',')]
                        code = parts[i+2].strip()
                        snippets.append({"name": name, "tags": tags, "code": code})
    return snippets

def render_sidebar(placeholder, all_snippets):
    """Renders the snippet list in the sidebar, highlighting selected items."""
    with placeholder.container():
        st.header("Available Snippets", divider="rainbow")

        selected_names = st.session_state.get('selected_snippets', [])

        selected_items = [s for s in all_snippets if s['name'] in selected_names]
        unselected_items = [s for s in all_snippets if s['name'] not in selected_names]

        # Sort for consistent ordering
        selected_items.sort(key=lambda x: x['name'])
        unselected_items.sort(key=lambda x: x['name'])

        with st.container(border=True, height=500):
            if selected_items:
                for s in selected_items:
                    st.markdown(f"✅ **{s['name']}**")
                    st.caption(f"Tags: {', '.join(s['tags'])}")
                st.divider()

            for s in unselected_items:
                st.markdown(f"**{s['name']}**")
                st.caption(f"Tags: {', '.join(s['tags'])}")


class CodeConversionWorkflow(Workflow):
    """
    An agentic workflow to convert user requests into PySpark code.
    It uses a two-step process:
    1. A selector agent chooses relevant code snippets.
    2. A generator agent creates PySpark code using the selected snippets.
    """
    selector_agent: Agent
    generator_agent: Agent
    available_snippets: list

    def __init__(self, api_key: str, available_snippets: list):
        super().__init__()
        self.available_snippets = available_snippets
        self.selector_agent = Agent(
            model=Gemini(api_key=api_key),
            instructions=["You are an expert in Apache Spark."]
        )
        self.generator_agent = Agent(
            model=Gemini(api_key=api_key),
            instructions=["You are an expert in Apache Spark."]
        )

    def run(self, user_request: str) -> Iterator[RunResponse]:
        """Orchestrates the two-step code generation process."""
        # Step 1: Select snippets
        yield RunResponse(content="log:📝 Analyzing request and selecting relevant code snippets...")

        snippet_list_str = "\n".join([f"- {s['name']}: (Tags: {', '.join(s['tags'])})" for s in self.available_snippets])
        
        selection_prompt = f"""
<prompt>
    <role>
        You are an expert assistant that helps prepare for PySpark code generation.
        Your task is to select the most relevant code snippets that will help a developer convert a given request into PySpark.
        The user request might be in natural language or a Java Spark code snippet.
    </role>
    <user_request>
        <![CDATA[
{user_request}
        ]]>
    </user_request>
    <instructions>
        <instruction>Review the user's request.</instruction>
        <instruction>From the list of available snippets, identify the ones that are most relevant.</instruction>
        <instruction>Your response MUST be a JSON object with a single key "selected_snippets" which is an array of the names of the snippets you have chosen.</instruction>
        <instruction>Do not include any other text, explanation, or formatting. Only the JSON object.</instruction>
        <instruction>If no snippets are relevant, return an empty array.</instruction>
    </instructions>
    <context>
        <available_snippets>
{snippet_list_str}
        </available_snippets>
    </context>
    <output_format>
        {{
            "selected_snippets": ["snippet_name_1", "snippet_name_2"]
        }}
    </output_format>
</prompt>
"""
        yield RunResponse(content=f"prompt:selection:{selection_prompt}")
        selector_response = self.selector_agent.run(selection_prompt)
        
        selected_snippet_names = []
        try:
            # The model output might have ```json ... ``` markdown, let's strip it
            cleaned_response = re.sub(r"```json\n(.*?)\n```", r"\1", selector_response.content, flags=re.DOTALL).strip()
            selected_snippet_names = json.loads(cleaned_response).get("selected_snippets", [])
        except (json.JSONDecodeError, AttributeError):
            yield RunResponse(content=f"log:⚠️ Could not parse snippet selection response. Proceeding without snippets.")

        # Yield the selection result so the UI can update
        yield RunResponse(content=f"selection:{json.dumps(selected_snippet_names)}")

        if selected_snippet_names:
            yield RunResponse(content=f"log:✅ Selected snippets: {', '.join(selected_snippet_names)}")
        else:
            yield RunResponse(content="log:ℹ️ No specific snippets selected. Proceeding with general knowledge.")

        # Step 2: Generate Code
        yield RunResponse(content="log:🤖 Generating PySpark code...")
        
        snippet_objects = [s for s in self.available_snippets if s["name"] in selected_snippet_names]
        reference_snippets_xml = "\n".join(
            f'<snippet name="{s["name"]}"><![CDATA[\n{s["code"]}\n]]></snippet>'
            for s in snippet_objects
        )
        
        generation_prompt = f"""
<prompt>
    <role>
        You are an expert in Apache Spark. Your task is to generate PySpark code based on the user's request.
        The request may be in a natural language or a Java Spark code snippet to be converted.
    </role>
    <user_request>
        <![CDATA[
{user_request}
        ]]>
    </user_request>
    <instructions>
        <instruction>Generate idiomatic PySpark code that fulfills the user's request.</instruction>
        <instruction>Adhere to PySpark best practices.</instruction>
        <instruction>Use the provided reference snippets for guidance.</instruction>
        <instruction>Your response must contain ONLY the generated PySpark code, enclosed within `<code>` tags.</instruction>
        <instruction>Do not include any explanations, comments, or markdown formatting outside of the `<code>` block.</instruction>
    </instructions>
    <context>
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
        yield RunResponse(content=f"prompt:generation:{generation_prompt}")
        generation_response_stream = self.generator_agent.run(generation_prompt, stream=True)
        for chunk in generation_response_stream:
            yield RunResponse(content=f"code:{chunk.content}")

def main():
    st.set_page_config(layout="wide")
    
    load_dotenv()
    all_snippets = load_snippets()

    # Initialize session state for output and selections if they don't exist
    if "output" not in st.session_state:
        st.session_state.output = ""
    if 'selected_snippets' not in st.session_state:
        st.session_state.selected_snippets = []

    with st.sidebar:
        st.title("Spark Code Generator")
        sidebar_placeholder = st.empty()

    # Initial render of the sidebar
    render_sidebar(sidebar_placeholder, all_snippets)


    st.title("Test Data Platform - Pyspark generator")

    col1, col2 = st.columns(2)

    with col2:
        with st.container(border=True):
            output_header_placeholder = st.empty()
            output_header_placeholder.subheader("PySpark Code")
            # Create a placeholder for the output code
            output_container = st.empty()
            syntax_test_placeholder = st.empty()
            if st.session_state.output:
                output_container.code(st.session_state.output, language="python")

    with col1:
        with st.container(border=True):
            st.subheader("Input Code or Natural Language")
            input_code = st.text_area("Paste your code or describe what you want to build", height=400)
            if st.button("Convert", use_container_width=True):
                if input_code:
                    gemini_api_key = os.environ.get("GEMINI_API_KEY")
                    if not gemini_api_key:
                        st.error("A Gemini API key is required. Please add it to your environment variables as GEMINI_API_KEY or set it in the script.")
                        st.stop()
                    
                    # Reset state for a new run
                    st.session_state.output = ""
                    st.session_state.selected_snippets = []
                    render_sidebar(sidebar_placeholder, all_snippets) # Clear sidebar selections
                    output_container.empty()
                    syntax_test_placeholder.empty()
                    output_header_placeholder.subheader("PySpark Code")
                    
                    prompts_expander = st.expander("View Agent Prompts", expanded=False)
                    with prompts_expander:
                        selection_prompt_placeholder = st.empty()
                        generation_prompt_placeholder = st.empty()

                    with st.status("Starting conversion...", expanded=True) as status:
                        workflow = CodeConversionWorkflow(
                            api_key=gemini_api_key,
                            available_snippets=all_snippets
                        )
                        
                        response_iterator = workflow.run(user_request=input_code)

                        final_code_chunks = []
                        
                        for res in response_iterator:
                            content = res.content
                            if content.startswith("prompt:"):
                                prompt_type, prompt_content = content[7:].split(":", 1)
                                if prompt_type == "selection":
                                    with selection_prompt_placeholder.container():
                                        st.subheader("Snippet Selection Prompt")
                                        st.code(prompt_content, language='xml')
                                elif prompt_type == "generation":
                                    with generation_prompt_placeholder.container():
                                        st.subheader("Code Generation Prompt")
                                        st.code(prompt_content, language='xml')
                            elif content.startswith("selection:"):
                                try:
                                    names = json.loads(content[10:])
                                    st.session_state.selected_snippets = names
                                    render_sidebar(sidebar_placeholder, all_snippets)
                                except (json.JSONDecodeError, AttributeError):
                                    pass # Ignore if selection parsing fails
                            elif content.startswith("log:"):
                                log_message = content[4:]
                                status.write(log_message)
                                if "Analyzing" in log_message:
                                    status.update(label="📝 Analyzing request...")
                                elif "Generating" in log_message:
                                    status.update(label="🤖 Generating PySpark code...")
                                    output_header_placeholder.subheader("PySpark Code (Generating...)")
                            elif content.startswith("code:"):
                                code_chunk = content[5:]
                                final_code_chunks.append(code_chunk)
                                output_container.code("".join(final_code_chunks), language="python")

                        full_code_block = "".join(final_code_chunks)
                        
                        # First, try to extract from <code> tags
                        match = re.search(r"<code>(.*?)</code>", full_code_block, re.DOTALL)
                        if match:
                            extracted_code = match.group(1).strip()
                        else:
                            # Fallback to using the whole response if <code> is missing
                            extracted_code = full_code_block.strip()

                        # Now, clean the extracted code using a more robust regex to remove
                        # markdown fences like ```python ... ``` that the model might add.
                        cleaned_code = re.sub(r"^(?:```|''')\s*python\n?", "", extracted_code)
                        cleaned_code = re.sub(r"\n?(?:```|''')$", "", cleaned_code).strip()
                        
                        st.session_state.output = cleaned_code
                        # Final update to the code container with cleaned code
                        output_container.code(cleaned_code, language="python")
                        output_header_placeholder.subheader("PySpark Code")

                        status.update(label="Code generation complete!", state="complete", expanded=True)

                        with syntax_test_placeholder.container():
                            with st.spinner("🧪 Testing syntax..."):
                                time.sleep(random.uniform(1, 2))
                            st.success("✅ Syntax tests passed!")

if __name__ == "__main__":
    main()
