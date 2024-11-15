# Character by Design: Exploring the Link Between Actor Traits and Movie Characters Archetypes

## Abstract

The project goal is to explore the relationship between actors' traits — such as age, gender, ethnicity — and the character archetypes they portray in films. By analyzing casting patterns, this project aims to find out how specific actor profiles consistently coincide with archetypal roles like heroes, villains, mentors, or lovers. Our goal is to uncover whether certain traits predispose actors to particular roles and identify any underlying biases in casting decisions. This research also explores how these patterns vary across different film industries and across time. Ultimately, we aim to tell the story of how an actor's characteristics shape their cinematic destiny, influencing not only their career trajectory but also how audiences perceive iconic characters on screen.

## Research Questions

1. **What Are the Most Common Character Archetypes in Movies?**\nWhat are the most prevalent character archetypes found in films, and how can we define and clusterize them based on existing papers and our own research? Identifying these archetypes involves exploring recurring character types and their roles within various plots.\n
2. **Which Actor Traits Correspond to Specific Archetypes?**\nWhich actor traits — such as age, gender, ethnicity, and other physical attributes — are typically associated with specific archetypes? For instance, are certain traits more frequently linked to roles like heroes, villains, or mentors? Investigating these correlations can reveal patterns in casting decisions.\n
3. **Do Casting Patterns Exhibit Biases Based on Actor Traits?**\nDo casting patterns exhibit biases based on actor traits like age, gender, or ethnicity? Are there noticeable trends in how certain demographics are cast in specific roles? Examining these patterns can shed light on potential biases within the casting industry.\n
4. **How Do Casting Trends Vary Across Genres and Film Industries?**\nHow do these casting trends vary across different genres and film industries, such as Hollywood compared to Bollywood? Are there differences in how actors are cast for similar archetypes in different cultural or geographic contexts? Comparing casting practices can highlight cultural influences on the film industry.\n
5. **How Did Casting Trends For Different Archetypes Vary Across Time?**\nCan we notice shifts in casting process based on the change of actor traits correspondence to archetypes? For example, how did the image of hero or villian change over time? Can we match these changes, if present, to some events in the world?\n
6. **Are Certain Actors More Likely to Be Typecast into Specific Roles?**\nAre certain actor profiles more likely to be typecast into specific roles? Do actors with particular traits find themselves repeatedly cast in similar roles throughout their careers? Analyzing actors' careers might reveal tendencies toward typecasting.\n
7. **What Is the Composite Image of the "Ideal" Actor for a Given Character Archetype?**\nWhat traits define the "ideal" actor for a character archetype? Can we develop actors profiles based on common traits? Understanding these ideals sheds light on industry standards and expectations.

## Additional Datasets

1. **Wikipedia**\nWe enhance our initial data with the Wikipedia API, resolving missing information. We collect actors' descriptions, including gender, ethnicity, and height, and movie descriptions. Movie descriptions are crucial for identifying characters' archetypes, as the main actors are typically highlighted.\n
2. **Freebase**\nWe used the dataset to extract actors' ethnicity, but due to its large format, we were unable to explore it fully. Perhaps we can extract more useful data from it in the future. \n
3. **IMDB** \nIMDB API is planned to be used to extract more unstructured information about movie plot and characters descriptions.\n
4. **[Paper: Learning Latent Personas of Film Characters](https://aclanthology.org/P13-1035.pdf)**\nThe paper's authors explore the categorization of movie characters based on their personas. Drawing from their findings, they propose a dataset of character archetypes. We leverage this data to refine our classification solution and evaluate its performance.

## Methods

1. **Additional Data Gathering**\nThe initial dataset is significantly affected by missing data. To enhance our archetype descriptions, we need more actors' characteristics. This step addresses missing information and adds actors' traits and movie descriptions to facilitate further analysis.\n
2. **Enriching Data Using Generative AI**\nAs movie persona archetypes are derived from data, we develop a method that extracts archetypes for each main actor in a movie using generative AI and a common question inference method, such as fine-tuning, few-shot answering, or zero-shot answering.\n
3. **Exploratory Data Analysis**

   This step involves exploring the data to address the proposed questions. It includes identifying relationships between features, tracking changes over time, and determining crucial archetypes for the project. It may also exclude or merge some archetypes.

## Proposed Timeline

<img width="1682" alt="Screenshot 2024-11-15 at 13 50 06" src="https://github.com/user-attachments/assets/b1a0b3a0-a762-4dda-8ce2-95162156a96e">


* **Enriching Data**\nWe will continue to gather more data about actors to develop more comprehensive archetypes. Furthermore, we will also collect data about movies to ensure that the AI has access to the most relevant context.
* **Archetypes Inference**\nWe use LM to predict and evaluate archetypes. This process is iterative as it is dependent with the data enrichment step.
* **Archetypes EDA**\nWe distribute the project questions among the team members and answer them based on the data we acquired.

## Organization within the team

Internal milestones are outlined in the "Proposed Timeline" section. Organisation of the team:

* **until P2**
  * Kirill Z — exploring LM solutions for predicting characters' archetypes and drafting a working example, showing that we can infer archetypes for new movies and actors.
  * Andrew — working on data collection pipeline for actors features.
  * Kirill A — working on data collection pipeline for movies features.
  * Seva — explores the initial dataset, guides data gathering for Kirill A and Andrew.
  * Alex — responsible for team coordination, research planning, this document drafting, and milestone result presentation.
* **until P3**
  * each member: EDA & answering the proposed questions.
  * Kirill Z — improving archetypes inference pipeline.
  * Kirill A, Andrew, Seva, Alex — improving data gathering pipelines and finalising data for EDA.

## Questions for TAs

1. Are we missing something that can improve our project?
2. Are we doing enough for the high project grading? If not, what can be improved?
3. Any suggestions of additional datasets that we missed?
4. Any suggestions for our working pipeline and data manipulation techniques?
