#!/bin/bash

# Build release individual platform packages
# Generates separate zip files for each WhatsApp chat, Facebook, and all combined

set -e

echo "Building WhatsApp Chat 1 release package..."
REACT_APP_RELEASE_PLATFORM=whatsapp:1 npm run build
cd build && zip -r ../release-whatsapp-1.zip . && cd ..
echo "✓ Created release-whatsapp-1.zip"

echo ""
echo "Building WhatsApp Chat 2 release package..."
REACT_APP_RELEASE_PLATFORM=whatsapp:2 npm run build
cd build && zip -r ../release-whatsapp-2.zip . && cd ..
echo "✓ Created release-whatsapp-2.zip"

echo ""
echo "Building WhatsApp Chat 3 release package..."
REACT_APP_RELEASE_PLATFORM=whatsapp:3 npm run build
cd build && zip -r ../release-whatsapp-3.zip . && cd ..
echo "✓ Created release-whatsapp-3.zip"

echo ""
echo "Building Facebook release package..."
REACT_APP_RELEASE_PLATFORM=facebook npm run build
cd build && zip -r ../release-facebook.zip . && cd ..
echo "✓ Created release-facebook.zip"

echo ""
echo "Building complete release package (all platforms)..."
REACT_APP_RELEASE_PLATFORM=all npm run build
cd build && zip -r ../release-all.zip . && cd ..
echo "✓ Created release-all.zip"

echo ""
echo "Release packages created successfully:"
ls -lh release-*.zip
