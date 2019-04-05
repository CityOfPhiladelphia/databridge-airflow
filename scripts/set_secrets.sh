# Run set_secrets.py and set outputs as environment variables
eval $(python3 set_secrets.py | sed 's/^/export /')
