#!/bin/bash
set -e

echo "Checking kind cluster..."
if ! kind get clusters | grep -q raft-cluster; then
	kind create cluster --name raft-cluster
fi

echo "Building images..."
docker build -f deploy/docker/Dockerfile.node -t raft-coordinator .
docker build -f deploy/docker/Dockerfile.replica -t raft-replica .

echo "Loading images into kind..."
kind load docker-image raft-coordinator:latest --name raft-cluster
kind load docker-image raft-replica:latest --name raft-cluster

echo "Applying manifests..."
kubectl apply -f deploy/k8s/coordinator-statefulset.yaml
kubectl apply -f deploy/k8s/replica-deployment.yaml

kubectl rollout restart statefulset/coordinator
kubectl rollout restart statefulset/replica

echo "Waiting for pods..."
kubectl wait --for=condition=ready pod -l app=coordinator --timeout=60s
kubectl wait --for=condition=ready pod -l app=replica --timeout=60s

echo "Cluster ready. Starting port-forwards..."
kubectl port-forward coordinator-0 8001:8001 &
kubectl port-forward coordinator-1 8002:8002 &
kubectl port-forward coordinator-2 8003:8003 &

echo "All set. Dashboard: open web/dashboard.html in your browser."
echo "Press Ctrl+C to stop port-forwards when done."
wait