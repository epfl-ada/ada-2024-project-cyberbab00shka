import re

import yaml
from langchain.schema import AIMessage
from langchain_core.messages import SystemMessage
from langchain_core.prompts import (ChatPromptTemplate,
                                    HumanMessagePromptTemplate)
from langchain_core.runnables.base import RunnableSequence
from langchain_google_vertexai import ChatVertexAI


def persona_capitalize(persona):
    return " ".join(map(lambda x: x.capitalize(), persona.split("_")))


def persona_lowercase(text):
    return re.sub(" ", "_", text).lower()


class PersonaIdentification(RunnableSequence):
    def __init__(self, config_path="persona_identification_config.yaml", **kwargs):
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        target_classes = config["classes"]

        system_prompt = config["system_prompt"]
        system_prompt += "\n".join(target_classes) + "\n"
        system_message = SystemMessage(content=system_prompt)
        chat_prompt = ChatPromptTemplate.from_messages(
            [
                system_message,
                HumanMessagePromptTemplate.from_template(config["prompt_template"]),
            ]
        )

        llm = ChatVertexAI(
            model_name=config["model_name"],
            tuned_model_name=config["endpoint"],
            **kwargs
        )

        def process_ai_message(ai_message: AIMessage):
            content = ai_message.content
            matched = False
            for unique_pers in target_classes:
                if unique_pers in content:
                    ai_message.content = unique_pers
                    matched = True
                    break
            ai_message.response_metadata["parsing_success"] = matched

            return ai_message

        model = chat_prompt | llm | process_ai_message
        super().__init__(model)
