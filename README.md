A rough prototype for Hydroflow consistency analysis, in the spirit of [Bloom](https://bloom-lang.net)'s [`budplot`](https://github.com/bloom-lang/bud/blob/master/bin/budplot) tool.

Input should be a `.dot` file, as produced by the Hydroflow method `serde_graph().to_dot()`. The output is a file `taint.dot` that is a recoloring of the input graph. The legend should be as follows:

Edges:
- Solid vs Dotted edges: deterministic vs non-deterministic ordering
- Green vs Red edges: monotone vs non-monotone sources

Nodes:
- Solid vs Dotted border: deterministic vs non-deterministic ordering
- Green border: monotone
- Blue/Yellow/Gray fill: consistent (untainted). These colors come from Hydroflow (Blue is pull, yellow is push, gray is handoff).
- Red fill: Inconsistent (tainted), i.e. downstream of non-determinism AND non-monotocity -- hence inconsistent (across runs or replicas)

Example:

```bash
% python3 flowrules.py graph-joins.dot
% dot -Tpdf taint.dot > taint.pdf
% open taint.pdf
```
I also recomment the [VScode extension for Graphviz](Name: Graphviz (dot) language support for Visual Studio Code
Id: joaompinto.vscode-graphviz
Description: This extension provides GraphViz (dot) language support for Visual Studio Code
Version: 0.0.6
Publisher: João Pinto
VS Marketplace Link: https://marketplace.visualstudio.com/items?itemName=joaompinto.vscode-graphviz) to save the trouble of running dot and generating pdfs.