#
# Optimizing pipelines.
#

import Base:
    append!,
    convert,
    delete!,
    getindex,
    isempty,
    merge,
    prepend!,
    size


#
# Mutable representation of the pipeline tree.
#

mutable struct Chain{T}
    up::Union{T,Nothing}
    up_idx::Int
    down_head::Union{T,Nothing}
    down_tail::Union{T,Nothing}

    Chain{T}() where {T} =
        new(nothing, 0, nothing, nothing)
end

isempty(c::Chain{T}) where {T} =
    c.down_head === c.down_tail === nothing

mutable struct PipelineNode
    op
    args::Vector{Any}
    left::Union{PipelineNode,Nothing}
    right::Union{PipelineNode,Nothing}
    up::Union{Chain{PipelineNode},Nothing}
    down::Union{Chain{PipelineNode},Nothing}
    down_many::Union{Vector{Chain{PipelineNode}},Nothing}

    PipelineNode(op, args::Vector{Any}=Any[]) =
        new(op, args, nothing, nothing, nothing, nothing, nothing)
end

const PipelineChain = Chain{PipelineNode}

@inline size(p::PipelineNode) = size(p.down_many)

@inline getindex(p::PipelineNode) = p.down

@inline getindex(p::PipelineNode, k::Number) = p.down_many[k]

function delete!(p::PipelineNode)
    l = p.left
    r = p.right
    u = p.up
    if l !== nothing
        @assert l.right === p
        if r === nothing
            @assert l.left !== nothing || l.up === u
            l.up = u
        end
        l.right = r
    end
    if r !== nothing
        @assert r.left === p
        if l === nothing
            @assert r.right !== nothing || r.up === u
            r.up = u
        end
        r.left = l
    end
    if u !== nothing
        @assert l === nothing || r === nothing
        if l === nothing
            @assert u.down_head === p
            u.down_head = r
        end
        if r === nothing
            @assert u.down_tail === p
            u.down_tail = l
        end
    end
    p.left = p.right = p.up = nothing
end

function append!(c::PipelineChain, p::PipelineNode)
    t = c.down_tail
    if t !== nothing
        @assert t.right === nothing && t.up === c
        t.right = p
        if t.left !== nothing
            t.up = nothing
        end
    end
    @assert p.left === p.right === p.up === nothing
    p.left = t
    p.up = c
    if c.down_head === nothing
        c.down_head = p
    end
    c.down_tail = p
end

function append!(l::PipelineNode, p::PipelineNode)
    r = l.right
    u = l.up
    l.right = p
    if l.left !== nothing
        l.up = nothing
    end
    if r !== nothing
        @assert r.left === l
        r.left = p
    else
        @assert u === nothing || u.down_tail === l
        if u !== nothing
            u.down_tail = p
        end
    end
    @assert p.left === p.right === p.up === nothing
    p.left = l
    p.right = r
    if r === nothing
        p.up = u
    end
end

function prepend!(r::PipelineNode, p::PipelineNode)
    l = r.left
    u = r.up
    r.left = p
    if r.right !== nothing
        r.up = nothing
    end
    if l !== nothing
        @assert l.right === r
        l.right = p
    else
        @assert u === nothing || u.down_head === r
        if u !== nothing
            u.down_head = p
        end
    end
    @assert p.left === p.right === p.up === nothing
    p.left = l
    p.right = r
    if l === nothing
        p.up = u
    end
end


#
# Conversion between mutable and immutable representations.
#

function convert(::Type{Pipeline}, p::PipelineNode)::Pipeline
    args = copy(p.args)
    if p.down !== nothing
        push!(args, convert(Pipeline, p.down))
    end
    if p.down_many !== nothing
        push!(args, Pipeline[convert(Pipeline, c) for c in p.down_many])
    end
    Pipeline(p.op, args=args)
end

function convert(::Type{Pipeline}, c::PipelineChain)::Pipeline
    if c.down_head === c.down_tail === nothing
        pass()
    elseif c.down_head === c.down_tail
        convert(Pipeline, c.down_head)
    else
        chain = Pipeline[convert(Pipeline, c.down_head)]
        p = c.down_head
        while p !== c.down_tail
            p = p.right
            @assert p !== nothing
            push!(chain, convert(Pipeline, p))
        end
        chain_of(chain)
    end
end

function convert(::Type{PipelineChain}, p::Pipeline)::PipelineChain
    if p.op === pass && isempty(p.args)
        PipelineChain()
    elseif p.op === chain_of && length(p.args) == 1 && p.args[1] isa Vector{Pipeline}
        c = PipelineChain()
        for q in p.args[1]
            c′ = convert(PipelineChain, q)
            if c.down_head === c.down_tail === nothing
                c = c′
            elseif !(c′.down_head === c′.down_tail === nothing)
                c.down_tail.up = nothing
                c.down_tail.right = c′.down_head
                c′.down_head.up = nothing
                c′.down_head.left = c.down_tail
                c.down_head.up = c
                c′.down_tail.up = c
                c.down_tail = c′.down_tail
            end
        end
        c
    else
        args = copy(p.args)
        down_many = nothing
        if !isempty(args) && args[end] isa Vector{Pipeline}
            qs = pop!(args)
            down_many = PipelineChain[convert(PipelineChain, q) for q in qs]
        end
        down = nothing
        if !isempty(args) && args[end] isa Pipeline
            q = pop!(args)
            down = convert(PipelineChain, q)
        end
        p = PipelineNode(p.op, args)
        if down !== nothing
            down.up = p
            p.down = down
        end
        if down_many !== nothing
            for (n, c) in enumerate(down_many)
                c.up = p
                c.up_idx = n
            end
            p.down_many = down_many
        end
        c = PipelineChain()
        p.up = c
        c.down_head = c.down_tail = p
        c
    end
end


#
# Multi-pass pipeline optimizer.
#

function rewrite_all(p::Pipeline)::Pipeline
    sig = signature(p)
    c = convert(PipelineChain, p)
    rewrite_all!(c, sig)
    p′ = convert(Pipeline, c) |> designate(sig)
    p′
end

function rewrite_all!(c::PipelineChain, sig::Signature)
    rewrite_simplify!(c)
end

function rewrite_with!(f!, p::PipelineNode)
    if p.down !== nothing
        f!(p.down)
    end
    if p.down_many !== nothing
        for c in p.down_many
            f!(c)
        end
    end
end

function rewrite_with!(f!, c::PipelineChain)
    p = c.down_head
    while p !== nothing
        p′ = p.right
        f!(p)
        p = p′
    end
end


#
# Local simplification.
#

function rewrite_simplify(p::Pipeline)::Pipeline
    sig = signature(p)
    c = convert(PipelineChain, p)
    rewrite_simplify!(c)
    p′ = convert(Pipeline, c) |> designate(sig)
    p′
end

function rewrite_simplify!(c::PipelineChain)
    rewrite_with!(rewrite_simplify!, c)
end

function rewrite_simplify!(p::PipelineNode)
    rewrite_with!(rewrite_simplify!, p)
    simplify!(p)
end

function simplify!(p::PipelineNode)
    l = p.left
    # with_elements(pass()) => pass()
    if p.op === with_elements && p.down !== nothing && isempty(p.down)
        delete!(p)
    # with_column(k, pass()) => pass()
    elseif p.op === with_column && p.down !== nothing && isempty(p.down)
        delete!(p)
    # chain_of(p, filler(val)) => filler(val)
    elseif p.op === filler || p.op === block_filler || p.op === null_filler
        while p.left !== nothing
            delete!(p.left)
        end
    elseif l !== nothing
        # chain_of(tuple_of(p1, ..., pn), column(k)) => pk
        if l.op === tuple_of && p.op === column
            @assert length(l.args) == 1 && l.args[1] isa Vector{Symbol} && l.down_many !== nothing
            @assert length(p.args) == 1 && p.args[1] isa Union{Int,Symbol}
            lbls = l.args[1]::Vector{Symbol}
            lbl = p.args[1]::Union{Int,Symbol}
            k = lbl isa Symbol ? findfirst(isequal(lbl), lbls) : lbl
            @assert 1 <= k <= length(l.down_many)
            c = l.down_many[k]
            while !isempty(c)
                q = c.down_head
                delete!(q)
                prepend!(l, q)
                simplify!(q)
            end
            delete!(l)
            delete!(p)
        # chain_of(tuple_of(..., pk, ...), with_column(k, q)) => tuple_of(..., chain_of(pk, q), ...)
        elseif l.op === tuple_of && p.op === with_column
            @assert length(l.args) == 1 && l.args[1] isa Vector{Symbol} && l.down_many !== nothing
            @assert length(p.args) == 1 && p.args[1] isa Union{Int,Symbol} && p.down !== nothing
            lbls = l.args[1]::Vector{Symbol}
            lbl = p.args[1]::Union{Int,Symbol}
            k = lbl isa Symbol ? findfirst(isequal(lbl), lbls) : lbl
            @assert 1 <= k <= length(l.down_many)
            c = l.down_many[k]
            c′ = p.down
            while !isempty(c′)
                q = c′.down_head
                delete!(q)
                append!(c, q)
                simplify!(q)
            end
            delete!(p)
        # chain_of(tuple_of(chain_of(p, wrap()), ...), tuple_lift(f)) => chain_of(tuple_of(p, ...), tuple_lift(f))
        elseif l.op === tuple_of && p.op === tuple_lift
            @assert length(l.args) == 1 && l.down_many !== nothing
            for c in l.down_many
                if c.down_tail !== nothing && c.down_tail.op === wrap
                    delete!(c.down_tail)
                end
            end
        # chain_of(with_column(k, chain_of(p, wrap())), distribute(k)) => chain_of(with_column(k, p), wrap())
        elseif l.op === with_column && l.down !== nothing && !isempty(l.down) && l.down.down_tail.op === wrap &&
               p.op === distribute && length(l.args) == 1 && length(p.args) == 1 && l.args[1] == p.args[1]
            q = l.down.down_tail
            delete!(q)
            if isempty(l.down)
                delete!(l)
            end
            append!(p, q)
            delete!(p)
        # chain_of(wrap(), flatten()) => pass()
        elseif l.op === wrap && p.op === flatten
            delete!(l)
            delete!(p)
        # chain_of(wrap(), with_elements(p)) => chain_of(p, wrap())
        elseif l.op === wrap && p.op === with_elements
            c = p.down
            @assert c !== nothing
            while !isempty(c)
                q = c.down_head
                delete!(q)
                prepend!(l, q)
                simplify!(q)
            end
            delete!(p)
            simplify!(l)
        # chain_of(wrap(), lift(f)) => lift(f)
        elseif l.op === wrap && p.op === lift
            delete!(l)
        # chain_of(with_elements(p), with_elements(q)) => with_elements(chain_of(p, q))
        elseif l.op === with_elements && p.op === with_elements
            @assert l.down !== nothing && p.down !== nothing
            while !isempty(p.down)
                q = p.down.down_head
                delete!(q)
                append!(l.down, q)
                simplify!(q)
            end
            delete!(p)
        # chain_of(with_elements(chain_of(p, wrap())), flatten()) => with_elements(p)
        elseif l.op === with_elements && l.down !== nothing && !isempty(l.down) && l.down.down_tail.op === wrap && p.op === flatten
            delete!(l.down.down_tail)
            if isempty(l.down)
                delete!(l)
            end
            delete!(p)
        end
    end
end

