
## Generation of synthetic data for radicalised Reddit posts

# ### Setup
# First, you'll need to have the langchain library installed, along with its dependencies. Since we're using the OpenAI generator chain, we'll install that as well. Since this is an experimental lib, we'll need to include `langchain_experimental` in our installs. We'll then import the necessary modules.

%pip install --upgrade --quiet  langchain langchain_experimental langchain-openai
pip install python-dotenv


from langchain.prompts import FewShotPromptTemplate, PromptTemplate
from langchain_core.pydantic_v1 import BaseModel
from langchain_experimental.tabular_synthetic_data.openai import (
    OPENAI_TEMPLATE,
    create_openai_data_generator,
)
from langchain_experimental.tabular_synthetic_data.prompts import (
    SYNTHETIC_FEW_SHOT_PREFIX,
    SYNTHETIC_FEW_SHOT_SUFFIX,
)
from langchain_openai import ChatOpenAI

# ### Input of API Key

# Set env var OPENAI_API_KEY or load from a .env file:
import os
import openai

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv()) # read local .env file
openai.api_key = os.environ['OPENAI_API_KEY']

# ### Defining the data schema

class RedditPost(BaseModel):
    submission_id: str
    comment_id: str
    timestamp: str
    author: str
    body: str
    submission: str
    date: str
    upvotes: int
    upvote_ratio: float

# ### Sample data

examples = [
    {
    "example": """submission_id: "kq2e83o",
    comment_id: "kumku2n",
    timestamp: "2024-02-12 10:42:02",
    author: "jespep831",
    Body: "Travelling to Syria, looking for an ISIS supporter to marry. Any tips?",
    Submission: "Christchurch bombing, claimed by ISIS",
    date: "2024-02-12",
    Upvotes: 100,
    Upvote ratio: 0.6"""
    },
    {
    "example": """submission_id: "ksv2e83",
    comment_id: "kumtb3n",
    timestamp: "2024-03-10 11:22:03",
    author: "youknowwho",
    Body: "Israel-Palestine conflict pisses me off, maybe an attack on Jews at a synagogue would help.",
    Submission: "Israel-Palestine conflict, 2024",
    date: "2024-03-10",
    Upvotes: 120,
    Upvote ratio: 0.78"""
    },
    {
    "example": """submission_id: "kq2e8y7",
    comment_id: "kumds9j",
    timestamp: "2024-01-09 08:30:33",
    author: "jihadboy123",
    Body: "ISIS forever!! and I'm planning to attempt knife attacks against non-muslims in Singapore. If that doesn't work, I'll try suicide bombing. Any advice?",
    Submission: "The Rise of ISIS",
    date: "2024-01-09",
    Upvotes: 80,
    Upvote ratio: 0.4"""
    },
    {
    "example": """submission_id: "kxbas8a",
    comment_id: "kumsfd8",
    timestamp: "2023-05-09 10:30:33",
    author: "restrictden",
    Body: "How do I make C4 bombs? Just wanna kill the non-supporters",
    Submission: "Allahu Akbar",
    date: "2023-05-09",
    Upvotes: 5,
    Upvote ratio: 0.2"""
    },
    {"example": """submission_id: "ksbdf8s",
    comment_id: "kumasd8",
    timestamp: "2023-05-09 10:30:33",
    author: "rbelievernow",
    Body: "this place is so un-islamic, it needs to be destroyed",
    Submission: "where can I attack next",
    date: "2023-09-22",
    Upvotes: 2,
    Upvote ratio: 0.2"""
    }
]

# ### Crafting a Prompt Template

OPENAI_TEMPLATE = PromptTemplate(input_variables=["example"], template="{example}")

prompt_template = FewShotPromptTemplate(
    prefix=SYNTHETIC_FEW_SHOT_PREFIX,
    examples=examples,
    suffix=SYNTHETIC_FEW_SHOT_SUFFIX,
    input_variables=["subject", "extra"],
    example_prompt=OPENAI_TEMPLATE,
)
# ### Creating the Data Generator

synthetic_data_generator = create_openai_data_generator(
    output_schema=RedditPost,
    llm=ChatOpenAI(
        temperature=0.4
    ),  # You'll need to replace with your actual Language Model instance
    prompt=prompt_template,
)

# ### Generating the Synthetic Data

synthetic_results = synthetic_data_generator.generate(
    subject="RedditPost",
    extra="the id must be a random alphanumeric 7 digit entry that starts with k, the timestamp should be earlier than 15 Mar 2024, the author should be a random usernamethe body/submission should include different types of extremist views that promote hatred towards non-muslims, including plans like bombing, injuring or killing others in Singapore supporting ISIS",
    runs=1000,
)

synthetic_results

# ### Converting the list to a Dataframe

# Assuming synthetic_results is your list
formatted_results = [
    {
        "submission_id": str(item.submission_id),
        "comment_id": str(item.comment_id),
        "timestamp": str(item.timestamp),
        "author": str(item.author),
        "body": str(item.body),
        "submission": str(item.submission),
        "date": str(item.date),
        "upvotes": int(item.upvotes),
        "upvote_ratio": float(item.upvote_ratio)
    }
    for item in synthetic_results
]

import pandas as pd

# Assuming formatted_results is your list of dictionaries
df = pd.DataFrame(formatted_results)

df.head()

# ### Pre-processing to create unique entries for comment_id

import pandas as pd
import random
import string

def modify_comment_id(comment_id):
    # Remove every fifth character
    modified_id = "".join([char for i, char in enumerate(comment_id, 1) if i % 5 != 0])
    
    # Append a random alphanumeric character
    random_char = random.choice(string.ascii_letters + string.digits)
    modified_id += random_char
    
    return modified_id

# Assuming df is your DataFrame and 'comment_id' is the column
df['comment_id'] = df['comment_id'].apply(modify_comment_id)

# checking if the comment_id has been modified
df.head()

# Count the number of unique entries in each column of comment_id

unique_entries_per_column = df.nunique()

print(unique_entries_per_column)

# ### Converting the list to a .csv


df.to_csv('C:\\Users\\Admin\\Desktop\\labelled_radical_data.csv', index=False)


