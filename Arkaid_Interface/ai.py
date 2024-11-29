import os
from openai import OpenAI

def load_system_prompt():
    """Load the system prompt from file"""
    prompt_path = "prompt.txt"
    try:
        with open(prompt_path, 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        print(f"Warning: Prompt file not found at {prompt_path}")
        return """You are an expert SQL query generator. 
                Convert natural language questions into valid SQL queries.
                Return only the SQL query itself without any markdown formatting or backticks.
                The query should be properly formatted with appropriate spacing and line breaks.
                Do not include any explanations or comments."""

# Initialize OpenAI client with API key
client = OpenAI(
  base_url="https://api.together.xyz/v1",
    api_key="976ce91183d462ce86b63626a9422be9ac8829c91c66ef9d6517794744132d0c"
)

def generate_sql_query(user_input: str) -> str:
    """Generate SQL query from natural language input using OpenAI"""
    
    # Load system prompt from file
    system_prompt = load_system_prompt()
    
    # System prompt to guide the AI
    messages = [
        {
            "role": "system",
            "content": system_prompt
        },
        {
            "role": "user",
            "content": user_input
        }
    ]

    # Make API call to OpenAI
    try:
        chat_completion = client.chat.completions.create(
            messages=messages,
            model="meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo",
            temperature=0.1,
            max_tokens=500
        )
        
        # Extract and clean the generated SQL query
        sql_query = chat_completion.choices[0].message.content
        
        # Remove any markdown formatting or backticks if present
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
        
        return sql_query
        
    except Exception as e:
        print(f"Error generating SQL query: {str(e)}")
        return None

def generate_fun_fact(query_results) -> str:
    """Generate a gaming-related fun fact based on query results"""
    
    # Create a prompt that includes the query results
    system_prompt = """You are a gaming expert. Generate a short, interesting gaming-related fun fact based on the provided query results. 
    The fact should be directly related to the data shown and should provide additional context or interesting information.
    Keep the response under 2 sentences and make it engaging."""
    
    # Convert results to a readable format
    results_text = str(query_results)
    
    messages = [
        {
            "role": "system",
            "content": system_prompt
        },
        {
            "role": "user",
            "content": f"Generate a fun gaming fact based on these results: {results_text}"
        }
    ]

    try:
        chat_completion = client.chat.completions.create(
            messages=messages,
            model="meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo",
            temperature=0.7,
            max_tokens=150
        )
        
        return chat_completion.choices[0].message.content.strip()
        
    except Exception as e:
        print(f"Error generating fun fact: {str(e)}")
        return None

