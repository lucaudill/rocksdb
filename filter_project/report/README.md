# Project report (outline)

`main.tex` is a rough outline in the PVLDB style (`acmart`, `sigconf,nonacm`).
Bullets are notes to turn into prose; red `[TODO: ...]` marks show in the PDF,
and boxed placeholders stand in for figures that do not exist yet.

Experiment numbering used in the outline: E1-E5 as in `experiments.json`;
E6 = in-memory 1M-key DB, E7 = equal real bits/key + construction cost
(filter_bench), E8 = build cost during ingestion (optional).

## Overleaf

Overleaf ships `acmart`, so nothing else is needed. Build a zip with the
figures inside, then use New Project > Upload Project:

```bash
cd filter_project && rm -f report.zip && zip -j report.zip report/main.tex report/references.bib && zip report.zip -j figures/*.pdf
```

`\graphicspath` looks in `figures/` and `../figures/`, so the zip (flat) and a
local build from `report/` both find the figures. If you upload into a
`figures/` folder instead, that works too.

`references.bib` was written from memory; check each entry before submission.
