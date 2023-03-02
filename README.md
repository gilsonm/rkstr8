# RKSTR8

RKSTR8 (read *"orchestrate"*) is a general purpose, scalable, workflow definition language and execution platform for AWS.

Originally developed for large-scale data- and CPU-intensive computational biology workflows 
(see [Publications](#Publications)), it just
as well supports deep learning GPU-intensive pipelines and general batch processing workflows.

RKSTR8 entirely removes the operations burden of distributed computing (i.e. designing, setting up and tearing down networking, compute resources, security 
controls etc. in your cloud environment) so that you can focus on writing the functional components of your workflows.

At minimum, one need only specify the number of CPUs (and/or GPUs) and memory required for each step of your workflow.

Workflow steps are implemented by python functions. Packaged in Docker images, these functions can run any external 
programs they need to complete their work.

Steps are chained together by their dependencies into a Directed Acyclic Graph (DAG).
These dependencies are specificied in a declarative language in the easy-to-read and easy-to-edit YAML format.

Concurrent execution of steps, including entire sub-workflows, are determined by a combination of user-specification
and opportunities for parallelism discovered by a *best-effort* job scheduler.

One need only an AWS account and can simply `pip install rkstr8` and start writing workflows, even from the AWS Cloud Shell in the web browser.

Many workflows can be run in parallel on the same AWS account. Multiple workflows can combined in complex, non-linear ways by importing
RKSTR8 as a python module and using the simple `launch` API.

### Publications

1. Liang L, Fazel Darbandi S, Pochareddy S, Gulden FO, Gilson MC, Sheppard BK, Sahagun A, An JY, 
   Werling DM, Rubenstein JLR, Sestan N, Bender KJ, Sanders SJ.  
   **Developmental dynamics of voltage-gated sodium channel isoform expression in the human and mouse 
   brain.** *Genome Med*. 2021 Aug 23;13(1):135. doi: 10.1186/s13073-021-00949-0. PMID: 34425903; PMCID: PMC8383430.
2. Werling DM, Pochareddy S, Choi J, An JY, Sheppard B, Peng M, Li Z, Dastmalchi C, Santpere G, 
   Sousa AMM, Tebbenkamp ATN, Kaur N, Gulden FO, Breen MS, Liang L, Gilson MC, Zhao X, Dong S, 
   Klei L, Cicek AE, Buxbaum JD, Adle-Biassette H, Thomas JL, Aldinger KA, O'Day DR, Glass IA, 
   Zaitlen NA, Talkowski ME, Roeder K, State MW, Devlin B, Sanders SJ, Sestan N. **Whole-Genome and 
   RNA Sequencing Reveal Variation and Transcriptomic Coordination in the Developing Human 
   Prefrontal Cortex.** *Cell Reports*. 2020 Apr 7;31(1):107489. doi: 10.1016/j.celrep.2020.03.053.  PMID: 32268104; PMCID: PMC7295160.
3. An JY, Lin K, Zhu L, Werling DM, Dong S, Brand H, Wang HZ, Zhao X, Schwartz GB, Collins RL, 
   Currall BB, Dastmalchi C, Dea J, Duhn C, Gilson MC, Klei L, Liang L, Markenscoff-Papadimitriou E, 
   Pochareddy S, Ahituv N, Buxbaum JD, Coon H, Daly MJ, Kim YS, Marth GT, Neale BM, Quinlan AR, 
   Rubenstein JL, Sestan N, State MW, Willsey AJ, Talkowski ME, Devlin B, Roeder K, Sanders SJ. 
   **Genome-wide de novo risk score implicates promoter variation in autism spectrum disorder.** *Science*. 2018 Dec 14;362(6420):eaat6576. doi: 10.1126/science.aat6576. PMID: 30545852; PMCID: PMC6432922.
4. Werling DM, Brand H, An JY, Stone MR, Zhu L, Glessner JT, Collins RL, Dong S, Layer RM, 
   Markenscoff-Papadimitriou E, Farrell A, Schwartz GB, Wang HZ, Currall BB, Zhao X, Dea J, 
   Duhn C, Erdman CA, Gilson MC, Yadav R, Handsaker RE, Kashin S, Klei L, Mandell JD, 
   Nowakowski TJ, Liu Y, Pochareddy S, Smith L, Walker MF, Waterman MJ, He X, Kriegstein AR, 
   Rubenstein JL, Sestan N, McCarroll SA, Neale BM, Coon H, Willsey AJ, Buxbaum JD, Daly MJ, 
   State MW, Quinlan AR, Marth GT, Roeder K, Devlin B, Talkowski ME, Sanders SJ. **An analytical 
   framework for whole- genome sequence association studies and its implications for autism spectrum disorder.** 
   *Nature Genetics*. 2018 Apr 26;50(5):727-736. doi: 10.1038/s41588-018-0107-y.  
   PMID: 29700473; PMCID: PMC5961723.
