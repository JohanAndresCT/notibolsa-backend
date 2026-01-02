#!/bin/bash
set -e

echo "Desplegando NotiBolsa en Kubernetes..."

echo "Verificando Minikube..."
minikube status || minikube start

echo "Configurando Docker..."
eval $(minikube docker-env)

MINIKUBE_IP=$(minikube ip)
echo "Minikube IP: $MINIKUBE_IP"

echo ""
echo "Construyendo imágenes del backend..."

echo "   → commoncrawl-worker..."
docker build -t commoncrawl:latest ./commoncrawl-worker

echo "   → colcap-fetcher..."
docker build -t colcap:latest ./colcap-fetcher

echo "   → aggregator..."
docker build -t aggregator:latest ./aggregator

echo ""
echo "Construyendo frontend..."
docker build -t frontend:latest \
  --build-arg VITE_API_URL=http://${MINIKUBE_IP}:30082 \
  ../notibolsa-frontend

echo ""
echo "Aplicando manifiestos..."

kubectl apply -f ./k8s/commoncrawl.yaml
kubectl apply -f ./k8s/colcap.yaml
kubectl apply -f ./k8s/aggregator.yaml
kubectl apply -f ../notibolsa-frontend/deployment.yaml

echo ""
echo "Esperando pods..."
kubectl wait --for=condition=ready pod -l app=commoncrawl --timeout=120s
kubectl wait --for=condition=ready pod -l app=colcap --timeout=120s
kubectl wait --for=condition=ready pod -l app=aggregator --timeout=120s
kubectl wait --for=condition=ready pod -l app=frontend --timeout=120s

echo ""
echo "============================================"
echo "Despliegue completado"
echo "============================================"
echo ""
kubectl get pods
echo ""
kubectl get svc
echo ""
echo "URLs de acceso:"
echo "   Frontend:   http://${MINIKUBE_IP}:30080"
echo "   Backend:    http://${MINIKUBE_IP}:30082"
echo ""
echo "O ejecuta: minikube service frontend"
