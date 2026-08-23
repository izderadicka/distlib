# Engineering standards for project

# Project Philosophy
## Avoid Over Engineering

* Keep it simple stupid
* Implement exactly what's required by task specifications and acceptance criteria
* Cover specified edge cases but avoid adding extra functionality unless explicitly requested
* Focus on essential features - resist the urge to add bells and whistles
* Keep changes minimal and targeted - only modify what's necessary to fulfill requirements

## Cooperation

We are working together as team,  ask often, discuss program details, challenge my decisions as I''ll challenge yours.

Explain your decisions in detail, provide alternatives where appropriate.

# Use Idiomatic Rust Patterns

## Use Concise Control Flow

Prefer ? operator over explicit match statements for error propagation:

```
    // Good
    let result = operation()?;

    // Avoid
    match operation() {
        Ok(val) => val,
        Err(e) => return Err(MyError::from(e)),
    }
```

Use if let instead of match when handling single patterns:
```
    // Good
    if let Some(value) = option {
        do_something(value);
    }

    // Avoid
    match option {
        Some(value) => do_something(value),
        None => {}
    }
```

Use while let for loop patterns:
```
    // Good
    while let Some(item) = iterator.next() {
        process(item);
    }

    // Avoid
    loop {
        match iterator.next() {
            Some(item) => process(item),
            None => break,
        }
    }
```

Iterator Patterns
```
    Prefer iterator chains over manual loops:

    // Good
    let results: Vec<_> = items
        .iter()
        .filter(|item| item.is_valid())
        .map(|item| item.process())
        .collect();

    // Avoid
    let mut results = Vec::new();
    for item in &items {
        if item.is_valid() {
            results.push(item.process());
        }
    }
```

Use functional combinators (filter_map, flatten, fold, etc.) instead of intermediate collections

## Error Handling

Use ? operator over explicit error conversions when error types can be automatically converted via From trait or #[from] attribute:

```
    // Good: Use ? when NoteError has #[from] std::io::Error
    let file = File::open("data.txt")?;

    // Avoid: Explicit map_err when automatic conversion works
    let file = File::open("data.txt").map_err(NoteError::from)?;
```

Use map_err only when you need custom error transformation that cannot be handled by the From trait:

```
    // Good: Custom error context that can't be expressed via From
    let file = File::open("data.txt").map_err(|e| NoteError::IoWithContext(e, "failed to open config"))?;

    Prefer anyhow or thiserror for application-level error handling
    Use Result extensions like ok_or, and_then, or_else for complex flows
```

## Type System

- **MUST** leverage Rust's type system to prevent bugs at compile time
- **NEVER** use `.unwrap()` in library code; use `.expect()` only for invariant violations with a descriptive message
- **MUST** use meaningful custom error types with `thiserror`
- Use newtypes to distinguish semantically different values of the same underlying type
- Prefer `Option<T>` over sentinel values

## Error Handling

- **NEVER** use `.unwrap()` in production code paths
- **MUST** use `Result<T, E>` for fallible operations
- **MUST** use `thiserror` for defining error types and `anyhow` for application-level errors
- **MUST** propagate errors with `?` operator where appropriate
- Provide meaningful error messages with context using `.context()` from `anyhow`

## Function Design

- **MUST** keep functions focused on a single responsibility
- **MUST** prefer borrowing (`&T`, `&mut T`) over ownership when possible
- Limit function parameters to 5 or fewer; use a config struct for more
- Return early to reduce nesting
- Use iterators and combinators over explicit loops where clearer

## Struct and Enum Design

- **MUST** keep types focused on a single responsibility
- **MUST** derive common traits: `Debug`, `Clone`, `PartialEq` where appropriate
- Use `#[derive(Default)]` when a sensible default exists
- Prefer composition over inheritance-like patterns
- Use builder pattern for complex struct construction
- Make fields private by default; provide accessor methods when needed

## Other Idiomatic Patterns

* Prefer destructuring assignments where appropriate
* Use .. spread operator in struct updates
* Leverage From/Into traits for conversions
* Use AsRef/AsMut bounds for flexible parameter types
* Use qualified paths sparingly - prefer imports over fully-qualified names in function signatures

# Security

- **NEVER** store secrets, API keys, or passwords in code. Only store them in `.env`.
  - Ensure `.env` is declared in `.gitignore`.
- **MUST** use environment variables for sensitive configuration via `dotenvy` or `std::env`
- **NEVER** log sensitive information (passwords, tokens, PII)
- Use `secrecy` crate for sensitive data types

# Optimize code
All code you write MUST be fully optimized.

"Fully optimized" includes:

- maximizing algorithmic big-O efficiency for memory and runtime
- using parallelization and SIMD where appropriate
- following proper style conventions for Rust (e.g. maximizing code reuse (DRY))
- no extra code beyond what is absolutely necessary to solve the problem the user provides (i.e. no technical debt)

# Use standard Rust tooling

Cargo build, check, test, clippy


# Quality Assurance

Upon completion of each task that involves Rust code must compile without warnings and all tests must pass.

Also run these commands

## Format all Rust code
```
cargo fmt
```

## Run linter and static analysis
```
cargo clippy
```


## When to Run

* After completing any implementation task
* Before marking a task as done
* Before creating a pull request or committing changes

## Handling Check Errors

If cargo fmt or cargo clippy report issues:
* Fix all warnings and errors reported by clippy
* Re-run formatting if needed
* Verify the fixes don't break existing tests
* Re-run both commands to confirm compliance


# Git workflow

* create feature branches for individual changes
* content of feature branch should not be too bit - so it can me comfortably reviewed by me
* from feature branches create PRs and ask for review 
* master branch is only updated via PRs