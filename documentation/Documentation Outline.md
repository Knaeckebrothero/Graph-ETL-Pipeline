# Management summary
- We do this later once the documentations are written
# Introduction
- What is this project about (e.g. we have a waste disposal chatbot that can help people dipose their waste)
- The current system uses vector search and we wanned to try adding graph based search
- Will be the same for both documentations
## Initial situation
- The original chatbot had a "fixed pipeline like structure" (user input, chatbot searched vector db, determined whether a followup question should be asked, vector search again, formulation of the answer)
- The previous group had just migrated the chatbot to a langgraph based agent, but was still using the same strict guardrails
- The frontend had not been informed about the consequences of this decision which lead to the issue that the bot often took half a minute to answer (doing multiple vector searches while the UI didn't show any progress, rendering the bot as "unusable" as stated by the professor)
- Upon examining the knowledge base we also realized that there wasn't a whole lot to build upon (after we sorted and imported everything we ended up with around a 1000 waste items mostly from the Abfall ABC, and a few facilities, we have the initial numbers somewhere)
## Brief overview of the research landscape
- What are knowledge graphs
- What is the difference between vector search (dense/sparse retrival) and "graph search" (especially in the context of agents)
- Why a graph based solution is superior (e.g. the stuff from the initial html presentation why we wanned to try graph based RAG)
- Perhaps we can talk about how a solution could combine both systems
# Cleaning up the implementation (DE)
- Given the initial situation we decided that we would have to shift our focus towards building everything from scratch
- One of the first things we decided was to turn the chatbots biggest disadvantage into it's biggest strengt
- We decided to get rid of the strict guardrails and instead embrace the agentic behavior that previously rendered the chatbot as unusable
- Inspired by the reasoning chatbots of the major AI labs (in particular how gemini showed a expander that could be opened to reveal a high level view of the models thought process) we implemented a similar system.
## Usability aspects of Agentic chatbots
- Describe the implementation of the chatbots reasoning components that could visualize what the agent did
- How we switched to text streaming
- How we felt that waiting 30 seconds for an answer wasn't the issue overall, but that the old chatbot didn't gave any feedback until he was finished
- The new system not only showed the user that something was happening, but also allowed him to view how the agent was navigating the knowledge base to find an answer to the question.
## Extending the agents toolset
- Given that modern reasoning models where getting ever more capable we decided to increase the agents freedom
- We changed the primary model to a reasoning model (switched from gpt-4o to using gpt-oss-120b)
- We've added tools to query and navigate the neo4j database
- We've added websearch
- We added a vision helper (e.g. the ability to send images to a multimodal model to generate descriptions and answer questions, since the gpt-oss-120b wasn't multimodal)
- Building upon the vision helper we also build a EAN barcode scanner that would allow the user to scan barcodes of products
# Initial expenditure of the knowledge base (DM)
- Given the initial situation we decided that we would have to shift our focus towards building everything from scratch
- Originally we had planned to take the existing data, build a graph and then compare both systems to determine if a knowledge graph would perform better or in which areas the advantages would show
- Now after we had build a knowledge graph based Gon the existing data, we tested our agent and realized that the knowledge base simply didn't cover a lot of ground and the majority of queries simply ended up getting answered from the agents parameterized knowledge (e.g. the agent couldn't find what he was looking for because it wasn't there)
## Websearch as a dropin replacement
- In order to test the concept we decided to add websearch as a substitute for the lack of data
- Our tests showed that given basic websearch tools and enough "freedom" (e.g. steps, less strict prompts), the agent was able to answer every question in greater detail leading to a much better experience
- While websearch definitely wasn't the final solution, the ability to use a modern search engine on demand, mimicked the interaction pattern of using a vector store
- Experimenting with this approach also reinforced our believe in a graph based system, since our examples showed how the agent often started narrowing down his search, actively targeting details he had just looked up to explore possible answers to a question
- It proved that the agent was indeed able to navigate through a more or less structured environment to gather all the necessary details
## Agentic reasoning made the old guardrails obsolet
- Upon seeing how well the gpt-oss reasoning model handled even complex multi step question we realized that striping away the corset was the right desicion
- Not only did the agent need less strict instructions due to his internal reasoning (e.g. the model actively reasoned about which tool to use, requiring less guidance)
- Getting rid of the fixed structure allowed the agent to adapt to the situation, leading to better follow up questions or graceful recovering from knowledge gaps in the database by using websearch as a substitute (e.g. check the graph first, fall back to websearch for additional refinement)
- We witnessed the agent doing up to 30 tool calls to find the solution
# Designing the graph database schema (DE)
- I think we have a document on that, based of which we created the first version of the schema
- We decided to go with a ontology based graph schema or something like that (I'd have to look up the details for this part)
- The idea was to build the schema centered on waste items and waste streams (e.g. what item are we looking at and which steps do I have to do on the path of it's disposal)
- Later on we realized that we needed multilingual support so we added synonymes and so on
- Then upon working with products we realized that we needed to include a product layer since both "Nutella 750g" and "Corny Schoko" would be waste item chocolate, thought Nutella actually includes the glas, the nougat creme and the cap
- With our initial schema it would have been up to the AI model to realize that this product would be composed off multiple different waste items
- So we added a product layer which split products into multiple components which them selfs could either be waste items or split even further
# The search for new sources (DM)
- While giving the agent access to websearch had helped developing the UI extension and evaluating the model capabilities
- We knew that our knowledge base was severely lacking in both quality and quantity
- So, we designed a search strategy by looking at the schema and defining search/topic areas
- We then collected sources for each of these search area and then combined everything into a giant list
- We ended up with over 500 sources which after rough sorting and duplication brought us down to ~400 sources of which we deemed around ~50 to be very relevant
- The sources varied quite drastically (from chemical databases with 11 million substances, to pdfs about legal aspects of disposal)
- The only area we didn't include were university specific sources since that would have required a lot of manual labor (interviews, asking people, etc.)
- The previous semesters had already tried that with mixed results, which is why we focused on collecting foundational data (e.g. fill the graph with general waste items first, then try to add the local disposal sites and waste streams)
- In the end the majority of products could be disposed in waste bins and only special cases (chemicals, or complex/irregular items would require qualified personal or special waste disposal sites)
# Creating the dictionary baseline (DE)
- After we realized that our research yielded far more data than we expected (~400 data sources with thousands or sometimes even millions of waste items per source), we had essentially solved the data quantity problem.
- The next step would be to solve the quality issue, because while we had a lot of high quality information, the majority of it was either unstructured or contained lots of unnecessary information.
- So, instead of gradually building the graph by importing data sources, we came up with the idea to use a dictionary as the baseline
- This would get us the multilingual translations and keywords we needed as well as 
# ELT & LLM pipelines (DM)
- We can shortly describe all our pipelines
- We can talk about the generalistic llm pipeline concept and the implementation (chain and agent)
- We can talk about how we used a custom model and groq or what models we used for data processing
- And we can talk about how we used the pipelines for the different data sources (e.g. dictionary, avv codes, open food facts)
- We can give a short how to guide on how to use the pipelines to import new data sources (e.g. the steps, how to design prompts, etc.)




# Infrastructure (DE)
- All the deployment stuff
- How to setup pipelines
- How to access the graph
# Data imports (DM)
- Due to time and resource constraints we couldn't assess let alone import all of the ~400 data sources we had discovered.
- Which is why we decided to focus on building a foundation upon later semesters could build upon
## Dictionary (I forgot the name)
- The dictionary we imported should form the baseline and cover the language/discovery aspects of the disposal graph
- Describe how we extracted, transformed and imported the dictionary
- Implementation details and screenshots for details
## Open Food Facts
- Open Food Facts was chosen due to it's relevance in our waste disposal scenario (food items most likely to be disposed, etc.)
- Describe how we extracted, transformed and imported the dataset
- See implementation docs for details
## Office Items
- We wanned to import a Open Food Facts equivalent for every day office items (pencils, printer ink, glue)
- Unfortunately we couldn't get our hands on any of the B2B catalog
- So what we did was to import a few open alternatives (should be in the implementation docs)
- Solutions for later on could be to ask to university it self for the B2B catalog it has access to through it's suppliers since that would also contain all the items the university is purchasing and needs to provide a guide on how to recycle
## AVV codes
- Describe how we extracted, transformed and imported the categories
- Describe what issues we faced during the classification using llms
- Show how we classified 500 nodes using llms
- We didn't do the full database yet, since that would take a week but can be done easily after we have submitted the documentation
- See implementation docs for details




# Results
- Shared or perhaps do two results focused on the different aspects???

# Future Work / Roadmap
## Completing the AVV classification
- We classified ~500 nodes as a proof of concept using LLMs
- The full database can be processed with the same pipeline, estimated ~1 week of compute
- Once classified, AVV codes provide the legal backbone connecting waste items to official German waste categories

## Remaining data source integration
- Of our ~400 researched sources, only a fraction has been imported (dictionary baseline, OFF, AVV codes)
- Next priority sources (based on our relevance ranking):
  - Chemical substance databases (e.g. the 11M substance database we found) for hazardous waste classification
  - Municipal-specific data (Frankfurt FES waste guides, local regulations)
  - University-specific sources (campus bin locations, room assignments, building-specific rules)
- Each source category maps to existing schema layers — no schema changes needed for most imports

## Solving the B2B catalog / office items gap
- We couldn't find an open equivalent to Open Food Facts for everyday office/industrial items
- The BMEcat standard (used by German B2B suppliers) would be ideal but catalogs are proprietary
- Practical path: ask the university's procurement department for their supplier catalogs
- These catalogs would contain exactly the items the university purchases and needs disposal guidance for
- Alternative: scrape publicly available product data from office supply retailers

## EAN barcode deployment
- The barcode module is built and tested but not yet integrated into the live chatbot
- Integration plan: add barcode lookup as an agent tool (scan image → resolve product → traverse graph for per-component disposal)
- The material taxonomy and product import pipeline are ready, just need the agent tool wiring

## Scaling the product graph
- Current: 4.29M products from OFF Parquet dump, ~555K packaging components
- Next: Open Beauty Facts bulk import (cosmetics/personal care)
- Long-term: integrate UPCitemdb or similar for non-food/non-cosmetic products
- The LLM-based Product → WasteItem matching (Phase 2 agent) would dramatically increase the usefulness of the product graph by connecting products to disposal knowledge

## Dictionary baseline completion
- The download/parse/filter pipeline is built
- LLM classification (is_physical, category) has been designed but not yet run at full scale
- Estimated cost: under $5 for ~80k concept clusters via Groq
- Once completed, provides ~30-60k WasteItem anchor nodes for entity resolution during LLM extraction

