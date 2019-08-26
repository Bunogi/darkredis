# 0.2.2
- Update dependencies
# 0.2.1
- Fix compilation error on latest nightly
# 0.2.0
- Command and CommandList no longer perform any copies
- Added args method to Command and CommandList
- `lpush` and `rpush` now take multiple arguments
- Support password auth
- `lrange` no longer returns an Option, returns an empty vec instead.
- Add convenience functions for the following commands: `lset`, `ltrim`
# 0.1.3
- Remove unnecesarry generic parameter from lpop and rpop methods.
# 0.1.2
- Fix a couple documentation errors
# 0.1.1
- Initial release
