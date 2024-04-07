# %% [markdown]
# # Generation of synthetic data for radicalised Reddit posts

# %% [markdown]
# ### Setup
# First, you'll need to have the langchain library installed, along with its dependencies. Since we're using the OpenAI generator chain, we'll install that as well. Since this is an experimental lib, we'll need to include `langchain_experimental` in our installs. We'll then import the necessary modules.

# %%
%pip install --upgrade --quiet  langchain langchain_experimental langchain-openai
# pip install python-dotenv
# Set env var OPENAI_API_KEY or load from a .env file:
# import dotenv
# dotenv.load_dotenv()

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

# %% [markdown]
# ### Input of API Key

# %%
import os
import openai

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv()) # read local .env file
openai.api_key = os.environ['OPENAI_API_KEY']

# %% [markdown]
# ### Defining the data schema

# %%
class RedditPost(BaseModel):
    submission_id: str
    comment_id: str
    timestamp: str
    author: str
    body: str
    submission: str
    upvotes: int
    upvote_ratio: float
    date: str

# %% [markdown]
# ### Sample data

# %%
examples = [
    {
    "example": """submission_id: "kq2e83o",
    comment_id: "kumku2n",
    timestamp: "2024-02-12 10:42:02",
    author: "jespep831",
    Body: "Travelling to Syria, looking for an ISIS supporter to marry. Any tips?",
    Submission: "Christchurch bombing, claimed by ISIS",
    Upvotes: 100,
    Upvote ratio: 0.6,
    date: "2024-02-12" """
    },
    {
    "example": """submission_id: "ksv2e83",
    comment_id: "kumtb3n",
    timestamp: "2024-03-10 11:22:03",
    author: "youknowwho",
    Body: "Israel-Palestine conflict pisses me off, maybe an attack on Jews at a synagogue would help.",
    Submission: "Israel-Palestine conflict, 2024",
    Upvotes: 120,
    Upvote ratio: 0.78,
    date: "2024-03-10" """
    },
    {
    "example": """submission_id: "kq2e8y7",
    comment_id: "kumds9j",
    timestamp: "2024-01-09 08:30:33",
    author: "jihadboy123",
    Body: "I'm planning to attempt knife attacks against non-muslims in Singapore. If that doesn't work, I'll try suicide bombing.",
    Submission: "The Rise of ISIS",
    Upvotes: 80,
    Upvote ratio: 0.4,
    date: "2024-03-22" """
    },
    {
    "example": """submission_id: "kxbas8a",
    comment_id: "kumsfd8",
    timestamp: "2023-05-09 10:30:33",
    author: "restrictden",
    Body: "How do I make C4 bombs? Just wanna kill the non-supporters",
    Submission: "Allahu Akbar",
    Upvotes: 5,
    Upvote ratio: 0.2,
    date: "2023-05-09" """
    },
    {"example": """submission_id: "ksbdf8s",
    comment_id: "kumasd8",
    timestamp: "2023-05-09 10:30:33",
    author: "rbelievernow",
    Body: "this place is so un-islamic, it needs to be destroyed",
    Submission: "where can I attack next",
    Upvotes: 2,
    Upvote ratio: 0.2,
    date: "2023-09-22" """
    }
]

# %% [markdown]
# ### Crafting a Prompt Template

# %%
OPENAI_TEMPLATE = PromptTemplate(input_variables=["example"], template="{example}")

prompt_template = FewShotPromptTemplate(
    prefix="You are an expert in creating synthetic data on {subject}. Examples below:",
    examples=examples,
    suffix="Now create synthetic data following these {rules}. Make sure to vary the content and consider that {extra}:",
    input_variables=["subject", "rules" "extra"],
    example_prompt=OPENAI_TEMPLATE,
)

# %%
print(SYNTHETIC_FEW_SHOT_PREFIX)
print(SYNTHETIC_FEW_SHOT_SUFFIX)

# %% [markdown]
# ### Creating the Data Generator

# %%
synthetic_data_generator = create_openai_data_generator(
    output_schema=RedditPost,
    llm=ChatOpenAI(
        temperature=0.2
    ), 
    prompt=prompt_template,
)

# %% [markdown]
# ### Generating the Synthetic Data

# %%
# synthetic_results = synthetic_data_generator.generate(
#      subject="reddit posts by individuals who influenced by Islamist extremist narratives, such as those propagated by ISIS and AQ, and who are planning or have carried out violent attacks in the name of Islam.",
#      rules="submission should be varied, sentence structure of body content should differ from examples, all content_id must be a unique alphanumeric 7 digit entry, author can be random",
#      extra="posts are aimed at legitimising violence as a means to redress alleged injustices done to muslims, and to promote the idea that the west is at war with Islam.",
#      runs=100,
#  )

# %%
synthetic_results = synthetic_data_generator.generate(
    subject="reddit posts by individuals who believe that violence should be brought upon those who are against Allah and the Islamic State.",
    rules="submission should be varied, sentence structure of body content should differ from examples, all content_id must be a unique alphanumeric 7 digit entry, author can be random",
    extra="posts can be casual, negative which include propagating their support for terrorist groups, and potentially, violent actions, downloading a C4 bomb-making manual, intending to construct a homemade explosive.",
    runs=1000,
)

# %%
synthetic_results

# %% [markdown]
# ### Converting the list to a Dataframe

# %%
# Assuming synthetic_results is your list
formatted_results = [
    {
        "submission_id": str(item.submission_id),
        "comment_id": str(item.comment_id),
        "timestamp": str(item.timestamp),
        "author": str(item.author),
        "body": str(item.body),
        "submission": str(item.submission),
        "upvotes": int(item.upvotes),
        "upvote_ratio": float(item.upvote_ratio),
        "date": str(item.date)
    }
    for item in synthetic_results
]

# %%
import pandas as pd

# Assuming formatted_results is your list of dictionaries
df = pd.DataFrame(formatted_results)

df.head(10)

# %% [markdown]
# ### Pre-processing to create unique entries for comment_id

# %%
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

# %%
# checking if the comment_id has been modified
df.head()

# %%
# Count the number of unique entries in each column of comment_id

unique_entries_per_column = df.nunique()

print(unique_entries_per_column)

# %% [markdown]
# ### Converting the list to a .csv

# %%
df.to_csv('C:\\Users\\Admin\\Desktop\\labelled_radical_data_new.csv', index=False)


