In this repo, you will find several sub-repo. They are all data engineering /
data science project that have be coded automatically (by Anthropic models )
in various sessions in the past. They relate to different exemple and different
data type and data related issues. They have different architecture and 
may use different stacks (sometimes because the objective was more inclined
towards hand-rolling the idioms rather that importing existing tooling and
libraries).

In the aforementioned repo there are sometimes synthesis documents (either
intial plans or readme or companion document produced a posteriori), and 
there is also the code that has been produced according the various design
decisions that have been made while designing.

Here is your current objective now. I want to help me navigate the different
databases as an exercise to gain skills and design experience covering the 
different problem that may have to be adressed by a data engineer or back-end
software engineer for data centered application, or data scientist (with 
technical SWE skills). So the idea is that I take time reading those different
code bases to extract as much information / skills / knowledge / design
experience, as possible. This includes idoms, tooling practices, common libraries,
architectural and software engineering pattern, along the data pipeline :
ingestion, storage and modeling, orchestration and transformation, 
observation, data analysis and exploration, ML models and predictions,
serving and depoyement. And in terms of workbench and supporting languages, that
means : unix env settings, bash, makefiles and config files, SQL, python, ...
Whatever you feel is useful to master as a data engineer / analyst / scientist /
dev.

First pass I think is the following : for each of the codebase, read the 
different functions / modules / component / entrypoints / layers and 
produce a synthesis document of the codebase (more technical and that could
be read by a LLM, or someone who knows the stack with good expertise, so not 
someone who needs more verbosity and terminology and concepts and tools 
explaination for learning). The more "readable" / "easy" / verbose" document
will be the objective of a later production pass.

Second pass, once you have produce your technical synthesis document for each 
project / codebase, have a cross-read of those document to produce a single
document pointing comment patterns and ideas accross all projects / exercises and
also pointing differences and specificities of each project. So whatever is 
transferable / generic on one side, and whatever is specific and why a different
design decision may have been taken for a specific case on the other side.

Once this is done, the last objective will be to produce a general companion
document, pointing the main ideas, the main idioms, the interesting thing to know
and remember (as data scientist / engineer / developper necessary skills). In the
second part of this companion, different individual navigation / charts (entry
points, architecture, module, stack) for each of the different projects taken
individually.

There are 4 projects in total : "DE_pipeline_stream", "mediaElection27", 
"energy_predML", "refund-lab-autom". The last repo "refund-lab-source" is not 
really "handling" the data but producing the data : it is a synthetic generator
, so for "refund-lab-source" you may peek inside, but don't produce anything for
this one.