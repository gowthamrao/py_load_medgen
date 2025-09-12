## Agent Instructions

When running integration tests in this repository, you may encounter a `PermissionError` when the tests try to connect to the Docker daemon. This is because the current user does not have permission to access the Docker socket.

To fix this, you must add the current user to the `docker` group. You can do this by running the following command:

```bash
sudo usermod -aG docker $(whoami)
```

**Important:** After running this command, you must start a new shell session for the group membership to be updated. If you are in an interactive session, you can do this by exiting and starting a new one. If you are in a script, you may need to use a command like `su - $(whoami) -c "your_command"` to run a command in a new session.

Alternatively, you can use the `sg` command to run a command with the `docker` group's permissions: `sg docker -c "your_command"`. This is often a more reliable method in scripted environments.

The tests should then pass. Do not attempt to fix this by changing the permissions of the docker socket directly, as this is a security risk.
