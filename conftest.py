# Empty on purpose: its mere presence at the project root makes pytest add
# this directory to sys.path, so tests/test_*.py can `import firewall_proxy`,
# `import jira_client`, etc. directly, same as run_poc_loop.sh does.
