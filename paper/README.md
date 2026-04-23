# Concord Paper Draft

Working draft for VLDB 2027 (currently set up in SIGMOD/ACM `acmart sigconf` format).

## Contents

```
paper/
├── main.tex                 top-level document, includes everything
├── sections/
│   ├── intro.tex            §1  — drafted (full prose + contributions)
│   ├── related.tex          §2  — drafted (four clusters, with TODO bib verifies)
│   ├── motivation.tex       §3  — drafted (systematicity + cardinality-feedback negative)
│   ├── protocol.tex         §4  — drafted (the key section: model, declarations, cost model)
│   ├── implementation.tex   §5  — stubbed with outline
│   ├── evaluation.tex       §6  — stubbed with structure + existing post-hoc M1 numbers
│   ├── discussion.tex       §7  — stubbed with candidate threads
│   └── conclusion.tex       §8  — stub
├── figures/
│   └── system-diagram.tex   TikZ: Agent ↔ Session Harness + Workspace ↔ PostgreSQL
├── bib/
│   └── refs.bib             citations with `% VERIFY` comments on uncertain ones
└── README.md                this file
```

## Uploading to Overleaf

1. Zip this directory (or download the zip produced with the draft).
2. In Overleaf: **New Project → Upload Project** → select the zip.
3. Overleaf should compile `main.tex` automatically. If not, set main document in Menu → Main Document.

## Compiling locally (optional)

```
cd paper
pdflatex main
bibtex main
pdflatex main
pdflatex main
```

You'll see many bibtex warnings for entries with `\TODO` fields — expected while drafting.

## Workflow for iterating with Claude

The clean loop is **one section at a time**:

1. Pick a section to revise.
2. Copy the current `.tex` from Overleaf into the chat.
3. Tell Claude what to change.
4. Paste Claude's revision back into Overleaf.

For larger structural changes (add a section, restructure §4, etc.), download the Overleaf project as zip, upload here, Claude produces revised zip, re-upload to Overleaf.

## What's drafted vs. what's stubbed

**Drafted (prose complete, refinement expected):**
- §1 Introduction, including itemized contributions
- §2 Related Work, four clusters with positioning against each
- §3 Motivation, including cardinality-feedback negative result
- §4 Protocol and System Model, with TikZ diagram, three declarations and their legality conditions, and a cost-model sketch

**Stubbed with outline (drafted after experiments):**
- §5 Implementation
- §6 Evaluation (has structure + currently-known post-hoc M1 numbers)
- §7 Discussion
- §8 Conclusion

## Macros for evolving numbers

`main.tex` defines `\newcommand` macros for every headline number. Update the macro when a new measurement lands; the number updates everywhere in the paper:

```tex
\newcommand{\imdbSpeedupPostHoc}{1.30$\times$\xspace}  % measured
\newcommand{\imdbSpeedupPreHoc}{\placeholder{}}        % TODO: measure
\newcommand{\imdbSpeedupFull}{\placeholder{}}          % TODO: measure with M1+M2+M3
\newcommand{\ibSpeedup}{\placeholder{}}                % TODO: InsightBench headline
```

Unmeasured numbers render as orange `??` so they are visually conspicuous in the PDF until real values land.

## TODO markers

- `\TODO{...}` renders as red inline text in the PDF during review.
- `% VERIFY` comments in `refs.bib` flag bibtex entries whose details were LLM-drafted and need verification before submission.

## Title

Current: **Concord: Cooperative Query Optimization for LLM Agent Database Sessions**

Alternatives in `main.tex` header comment:
- Covenant: A Declared-Intent Protocol for Agent-Database Cooperation
- Agora: Shared Workspaces for Cooperative LLM Agent SQL Sessions
- Declared-Intent Cooperation: Optimizing LLM Agent Database Sessions at the Protocol Layer

Swap by editing the `\title{...}` line.

## Known issues to resolve early

1. **Bibliography verification.** Several `% VERIFY` entries in `refs.bib` were drafted from memory. Cross-check against DBLP before submission.
2. **Pre-hoc save number.** §1, §4, §6 all reference `\imdbSpeedupPreHoc` which is currently `??`. Replace the macro definition once the pre-hoc implementation is benchmarked.
3. **§4 cost model.** Currently a sketch. Formalize when drafting §5, since the implementation details of co-materialization constrain what the formal result can assume.
4. **InsightBench subset.** §6 references a curated `\placeholder{}`-size subset. Freeze the task list in May and fill in the number.
