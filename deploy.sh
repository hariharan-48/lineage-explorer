#!/bin/bash
# Deploy script for Lineage Explorer to GKE (Google Kubernetes Engine)
#
# Prerequisites:
#   1. Install gcloud CLI: https://cloud.google.com/sdk/docs/install
#   2. Install kubectl: gcloud components install kubectl
#   3. Login: gcloud auth login
#   4. Set project: gcloud config set project YOUR_PROJECT_ID
#   5. Configure docker: gcloud auth configure-docker
#
# Usage:
#   ./deploy.sh                    # Build images and deploy to GKE
#   ./deploy.sh --build-only       # Only build images, don't deploy
#   ./deploy.sh --deploy-only      # Only deploy (assumes images exist)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Get project ID
PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT" ] || [ "$PROJECT" == "(unset)" ]; then
    echo -e "${RED}Error: No GCP project set. Run: gcloud config set project YOUR_PROJECT_ID${NC}"
    exit 1
fi

# Configuration
REGION="${GKE_REGION:-us-central1}"
CLUSTER="${GKE_CLUSTER:-lineage-cluster}"
BACKEND_IMAGE="gcr.io/$PROJECT/lineage-backend"
FRONTEND_IMAGE="gcr.io/$PROJECT/lineage-frontend"
TAG="${IMAGE_TAG:-latest}"

echo "========================================"
echo "Lineage Explorer - GKE Deployment"
echo "========================================"
echo -e "Project:  ${GREEN}$PROJECT${NC}"
echo -e "Region:   ${GREEN}$REGION${NC}"
echo -e "Cluster:  ${GREEN}$CLUSTER${NC}"
echo ""

# Function to build images
build_images() {
    echo -e "\n${YELLOW}Building Docker images...${NC}"

    # Build backend
    echo -e "\n${YELLOW}Building backend image...${NC}"
    docker build -t "$BACKEND_IMAGE:$TAG" ./backend

    # Build frontend
    echo -e "\n${YELLOW}Building frontend image...${NC}"
    docker build -t "$FRONTEND_IMAGE:$TAG" ./frontend

    echo -e "${GREEN}Images built successfully${NC}"
}

# Function to push images
push_images() {
    echo -e "\n${YELLOW}Pushing images to GCR...${NC}"

    docker push "$BACKEND_IMAGE:$TAG"
    docker push "$FRONTEND_IMAGE:$TAG"

    echo -e "${GREEN}Images pushed successfully${NC}"
}

# Function to deploy to GKE
deploy_to_gke() {
    echo -e "\n${YELLOW}Deploying to GKE...${NC}"

    # Get cluster credentials
    echo -e "\n${YELLOW}Getting cluster credentials...${NC}"
    gcloud container clusters get-credentials "$CLUSTER" --region "$REGION" --project "$PROJECT"

    # Update image references in manifests
    echo -e "\n${YELLOW}Updating manifests with project ID...${NC}"
    sed -i.bak "s|gcr.io/PROJECT_ID/|gcr.io/$PROJECT/|g" k8s/*.yaml
    rm -f k8s/*.yaml.bak

    # Apply manifests
    echo -e "\n${YELLOW}Applying Kubernetes manifests...${NC}"
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/configmap.yaml
    kubectl apply -f k8s/backend-deployment.yaml
    kubectl apply -f k8s/frontend-deployment.yaml
    kubectl apply -f k8s/ingress.yaml

    # Wait for deployments
    echo -e "\n${YELLOW}Waiting for deployments to be ready...${NC}"
    kubectl rollout status deployment/lineage-backend -n lineage --timeout=120s
    kubectl rollout status deployment/lineage-frontend -n lineage --timeout=120s

    echo -e "${GREEN}Deployment complete!${NC}"
}

# Function to get ingress IP
get_ingress_ip() {
    echo -e "\n${YELLOW}Getting Ingress IP (may take a few minutes)...${NC}"

    for i in {1..30}; do
        IP=$(kubectl get ingress lineage-ingress -n lineage -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
        if [ -n "$IP" ]; then
            echo -e "\n${GREEN}========================================"
            echo "Deployment complete!"
            echo "========================================${NC}"
            echo ""
            echo -e "Your app is available at: ${GREEN}http://$IP${NC}"
            echo ""
            return 0
        fi
        echo "  Waiting for IP assignment... ($i/30)"
        sleep 10
    done

    echo -e "${YELLOW}Ingress IP not yet assigned. Check later with:${NC}"
    echo "  kubectl get ingress lineage-ingress -n lineage"
}

# Parse arguments
case "$1" in
    --build-only)
        build_images
        echo -e "\n${GREEN}Build complete! To push and deploy:${NC}"
        echo "  docker push $BACKEND_IMAGE:$TAG"
        echo "  docker push $FRONTEND_IMAGE:$TAG"
        echo "  ./deploy.sh --deploy-only"
        ;;
    --deploy-only)
        deploy_to_gke
        get_ingress_ip
        ;;
    *)
        build_images
        push_images
        deploy_to_gke
        get_ingress_ip
        ;;
esac

echo ""
echo "Useful commands:"
echo "  kubectl get pods -n lineage                    # List pods"
echo "  kubectl logs -f deployment/lineage-backend -n lineage   # Backend logs"
echo "  kubectl logs -f deployment/lineage-frontend -n lineage  # Frontend logs"
echo "  kubectl get ingress -n lineage                 # Ingress status"
