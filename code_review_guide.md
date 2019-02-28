# Code Review Guide

### Understand the code
- Make sure you completely understand the code
- Evaluate all the architecture tradeoffs
### Check code quality
- Verify code correctness
- Check for well-organized and efficient core logic
- Is the code as general as it needs to be, without being more general that it has to be?
- Make sure the code is maintainable
- Enforce stylistic consistency with the rest of the codebase
### Verify that the code is tested well
- Confirm adequate test coverage
- Check tests having the right dependencies and are testing the right things
### Make sure the code is safe to deploy
- Ask if the code is forwards/backwards compatible. In particular, be on the lookout if the code is changing the serialization / deserialization of something
- Run through a roll-back scenario to check for rollback safety
### Check for any security holes and loop in the security team if you’re unsure
### Answer: If this code breaks at 3am and you’re called to diagnose and fix this issue, will you be able to?