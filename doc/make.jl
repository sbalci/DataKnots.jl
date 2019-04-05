#!/usr/bin/env julia

using Pkg
haskey(Pkg.installed(), "Documenter") || Pkg.add("Documenter")
haskey(Pkg.installed(), "Literate") || Pkg.add("Literate")
haskey(Pkg.installed(), "Random") || Pkg.add("Random")
haskey(Pkg.installed(), "Distributions") || Pkg.add("Distributions")
haskey(Pkg.installed(), "Plots") || Pkg.add("Plots")

using Documenter
using DataKnots
using Literate

# Convert Literate example code to markdown.
INPUTS = joinpath(@__DIR__, "src", "simulation.jl")
OUTPUT = joinpath(@__DIR__, "src")
Literate.markdown(INPUTS, OUTPUT, documenter=true, credit=false)

# Highlight indented code blocks as Julia code.
using Markdown
Markdown.Code(code) = Markdown.Code("julia", code)

makedocs(
    sitename = "DataKnots.jl",
    format = Documenter.HTML(prettyurls=("CI" in keys(ENV))),
    pages = [
        "Home" => "index.md",
        "Manual" => [
            "tutorial.md",
            "reference.md",
        ],
        "Concepts" => [
            "primer.md",
            "vectors.md",
            "pipelines.md",
            "shapes.md",
            "knots.md",
            "queries.md",
        ],
        "Showcase" => [
            "simulation.md",
        ],
    ],
    modules = [DataKnots])

deploydocs(
    repo = "github.com/rbt-lang/DataKnots.jl.git",
)
