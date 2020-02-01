locals_without_parens = [
  event: 1,
  event: 2,
  field: 1,
  field: 2,
  field: 3,
  option: 1,
  option: 2,
  topic: 2,
  topic: 3,
  upgrade: 2,
  version: 1,
  version: 2
]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [
    locals_without_parens: locals_without_parens
  ]
]
