# Container start command
```
bash -lc "/workspace/start_user.sh && exec /sbin/docker-init -- /opt/nvidia/nvidia_entrypoint.sh /start.sh"
```

# Container image

```
runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04
```