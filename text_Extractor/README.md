Vehicle Oil Data PDF Extraction System

This module provides a complete pipeline for extracting vehicle oil specifications from PDF manuals
stored on Google Drive. It handles PDF discovery, text extraction, regex-based data mining, and
SQLite database storage.

Key Features:
- Google Drive API integration for automatic PDF discovery and download
- Advanced text parsing with 5+ regex patterns (oils, engines, temps, capacities, types)
- Multi-level temperature priority hierarchy (keywords > actual values > defaults)
- Per-engine oil recommendation mapping with confidence scoring
- Automatic fallback strategies for incomplete data
- Database persistence via SQLite with 5-table relational schema

Architecture:
1. Google Drive Integration: Discover and download PDF manuals
2. PDF Extraction: Text mining from PyMuPDF-parsed documents
3. Data Mining: Regex-based extraction of vehicle/engine/oil specifications
4. Inference Engine: Multi-step oil recommendation scoring and temperature mapping
5. Output Generation: JSON output and SQLite database persistence

Engine Support: 150+ codes (Honda, Toyota, Nissan, BMW, Volkswagen, Ford, etc.)
Vehicle Support: 50+ makes recognized with year/make/model parsing
Oil Specifications: Supports W-grade viscosity ratings (0W-60) with temperature conditions
Temperature Handling: Celsius ↔ Fahrenheit conversion, weather condition classification
