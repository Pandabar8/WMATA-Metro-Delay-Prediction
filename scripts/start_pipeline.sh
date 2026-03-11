#!/bin/bash
# Wrapper script for launchd to handle paths with spaces
cd "/Users/andresbarrientos/Desktop/Master/U of Maryland/Spring 26/ENAI_603/WMATA_Delays_Project/scripts"
exec "/Users/andresbarrientos/Desktop/Master/U of Maryland/Spring 26/ENAI_603/venv/bin/python3" run_pipeline.py
