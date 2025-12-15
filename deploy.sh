#!/bin/bash
# Deploy script for Lineage Explorer to Google App Engine
#
# Prerequisites:
#   1. Install gcloud CLI: https://cloud.google.com/sdk/docs/install
#   2. Login: gcloud auth login
#   3. Set project: gcloud config set project YOUR_PROJECT_ID
#
# Usage:
#   ./deploy.sh              # Build and deploy
#   ./deploy.sh --build-only # Only build, don't deploy

set -e

echo "========================================"
echo "Lineage Explorer - App Engine Deployment"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Step 1: Build frontend
echo -e "\n${YELLOW}Step 1: Building frontend...${NC}"
cd frontend
npm install
npm run build
cd ..

# Step 2: Copy frontend dist to backend/static
echo -e "\n${YELLOW}Step 2: Copying frontend build to backend/static...${NC}"
rm -rf backend/static
cp -r frontend/dist backend/static
echo -e "${GREEN}Frontend copied to backend/static${NC}"

# Step 3: Ensure requirements.txt has gunicorn
echo -e "\n${YELLOW}Step 3: Checking requirements.txt...${NC}"
if ! grep -q "gunicorn" backend/requirements.txt; then
    echo "gunicorn" >> backend/requirements.txt
    echo -e "${GREEN}Added gunicorn to requirements.txt${NC}"
fi
if ! grep -q "uvicorn" backend/requirements.txt; then
    echo "uvicorn[standard]" >> backend/requirements.txt
    echo -e "${GREEN}Added uvicorn to requirements.txt${NC}"
fi

# Step 4: Copy necessary files to backend for deployment
echo -e "\n${YELLOW}Step 4: Preparing deployment files...${NC}"
cp app.yaml backend/
cp .gcloudignore backend/ 2>/dev/null || true

# Check if build-only flag is set
if [ "$1" == "--build-only" ]; then
    echo -e "\n${GREEN}Build complete! Files ready in backend/ directory.${NC}"
    echo "To deploy manually, run: cd backend && gcloud app deploy"
    exit 0
fi

# Step 5: Deploy to App Engine
echo -e "\n${YELLOW}Step 5: Deploying to App Engine...${NC}"
cd backend

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI not found. Please install it first.${NC}"
    echo "Visit: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if user is logged in
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    echo -e "${RED}Error: Not logged in to gcloud. Run: gcloud auth login${NC}"
    exit 1
fi

# Check if project is set
PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT" ] || [ "$PROJECT" == "(unset)" ]; then
    echo -e "${RED}Error: No GCP project set. Run: gcloud config set project YOUR_PROJECT_ID${NC}"
    exit 1
fi

echo -e "Deploying to project: ${GREEN}$PROJECT${NC}"
echo ""

# Deploy
gcloud app deploy --quiet

echo -e "\n${GREEN}========================================"
echo "Deployment complete!"
echo "========================================${NC}"
echo ""
echo "Your app is available at:"
echo -e "  ${GREEN}https://$PROJECT.appspot.com${NC}"
echo ""
echo "Useful commands:"
echo "  gcloud app browse          # Open app in browser"
echo "  gcloud app logs tail       # View live logs"
echo "  gcloud app describe        # App info"
