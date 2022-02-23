# CI Development Notes

The CI system is designed around NixOS containers and runs using
`podman` on GitHub. It makes a number of assumptions that is
documented here. If you make changes, please update the new
assumptions here.

1. FoundationDB processes are run with a uid/gid of `4059` and
   username/groupname of `fdb`.

2. Within the container, we use uid/gid of `1001/121` to run workflow
   steps. This maps to username/groupname `runner/docker`. The uid/gid
   and username/groupname is the same on both the host ubuntu virtual
   machine and the container.

3. `GITHUB_WORKSPACE` is bind-mounted to `/home/runner/fdb`.
