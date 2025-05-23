# PyDough Type Inference

This subdirectory of the PyDough operators directory deals with utilities used for type inference and type verification in PyDough. The type inference and type verification tools are used to ensure that the arguments passed to operators in PyDough expressions are valid and to infer the return types of those expressions. This helps maintain type safety and correctness in PyDough operations.


## Available APIs

### [expression_type_deducer.py](expression_type_deducer.py)

- `ExpressionTypeDeducer`: Abstract base class for type-inferring classes that take in a list of PyDough expression QDAGs and return a PyDough type. Each implementation has an API `infer_return_type` that returns the inferred expression type based on the input arguments.
- `SelectArgumentType`: Type deduction implementation class that always selects the type of a specific argument from the inputs based on an ordinal position.
- `ConstantType`: Type deduction implementation class that always returns a specific PyDough type.

### [type_verifier.py](type_verifier.py)

- `TypeVerifier`: Abstract base class for verifiers that take in a list of PyDough QDAG objects and either silently accept them or reject them by raising an exception. Each implementaiton class  has an API  `accepts` that Verifies whether the type verifier accepts/rejects a list of arguments.
- `AllowAny`: Type verifier implementation class that always accepts, no matter the arguments.
- `RequireNumArgs`: Type verifier implementation class that requires an exact number of arguments.
- `RequireMinArgs`: Type verifier implementation class that requires a minimum number of arguments.
- `RequireArgRange`: Type verifier implementation class that requires the number of arguments to be between a minimum and maximum number inclusive on both ends.
- `RequireCollection`: type verifier implementation class that requires a single argument that is a collection, as opposed to an expression.

## Usage

To use the `type_inference` module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.pydough_operators.type_inference import (
    ConstantType,
    RequireNumArgs,
)
from pydough.types import NumericType

# Create a type verifier that requires exactly 0 arguments
num_args_verifier = RequireNumArgs(0)

# Create a type deducer that always returns numeric
constant_type_deducer = ConstantType(NumericType())

# An empty list of arguments
args = []

# Silently accepts the argument list
num_args_verifier.accepts(args)

# Returns the numeric type
return_type = constant_type_deducer.infer_return_type(args)
```
