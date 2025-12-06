# DTSEN Downloader Application

## Overview

The DTSEN Downloader is a sophisticated desktop application designed to automate the process of scraping sensitive social welfare data from the DTSEN (Data Terpadu Kesejahteraan Sosial) platform. It provides a user-friendly interface to input authorization credentials, fetch comprehensive family and individual data, and generate detailed reports in both Excel (`.xlsx`) and PDF formats.

The application is built with a modular and secure architecture, featuring a robust GUI, a decoupled backend orchestrator, and a modular scraping engine. It includes features for data encryption, payment processing, and detailed, multi-format report generation, making it a powerful tool for social welfare data analysis.

## Key Features

*   **Secure Credential Management:** Securely input and manage API credentials.
*   **Automated Data Scraping:** Fetches detailed family, member, asset, and social assistance (Bansos) data.
*   **Comprehensive Reporting:** Generates multi-sheet Excel reports with detailed breakdowns, master sheets, and data visualizations.
*   **Detailed PDF Summaries:** Creates in-depth, professionally formatted PDF reports for each family, including charts and data summaries.
*   **Data Visualization:** Automatically generates charts and graphs to provide insights into the scraped data.
*   **Modular Architecture:** A clean, decoupled architecture that separates the GUI, core logic, and scraping engine for maintainability and scalability.
*   **Secure File Handling:** Encrypts scraped data to protect sensitive information, with a secure unlocking mechanism.

## Technology Stack

*   **Backend:** Python 3
*   **GUI:** PySide6 (Qt for Python)
*   **HTTP Requests:** httpx
*   **Data Manipulation:** pandas
*   **Excel Reports:** XlsxWriter
*   **PDF Reports:** ReportLab
*   **Visualizations:** Matplotlib
*   **Encryption:** pycryptodome

## Project Structure

The application is organized into several key modules:

*   `main.py`: The main entry point for the application.
*   `app_gui.py`: The main GUI window and orchestrator of UI components.
*   `orchestrator.py`: The core backend logic that coordinates the scraping and reporting process.
*   `scrape_and_build.py`: The main interface for the scraping engine.
*   **`core/`**: Contains core utilities, including logging, error handling, and security components.
*   **`gui/`**: Contains all the modular GUI components, including custom widgets, stages, and workers.
*   **`scraper/`**: Contains the modular scraping engine, with separate components for API interaction, data processing, and report generation.
*   **`assets/`**: Contains static assets, such as icons and images.

## Setup and Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd dtsen-downloader-app
```

### 2. Create a Virtual Environment

It is highly recommended to use a virtual environment to manage project dependencies.

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

### 3. Install Dependencies

Install all the required Python packages using the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

The application requires several environment variables to be set for security and payment gateway integration. Create a `.env` file in the root of the project by copying the example file:

```bash
cp .env.example .env
```

Now, open the `.env` file and fill in the required values:

*   `MIDTRANS_SERVER_KEY`: Your Midtrans server key for payment processing.
*   `MIDTRANS_SANDBOX`: Set to `true` for testing or `false` for production.
*   `SECRET_SALT`: A unique, random string of at least 32 characters used for security operations.
*   `AES_BASE64_KEY`: The AES encryption key required for API payload encryption and decryption. This is a mandatory field.

The `.env` file also contains configurations for Supabase and the admin dashboard, which should be configured for full functionality.

## How to Run

Once you have completed the setup, you can run the application using the following command:

```bash
python main.py
```

This will launch the DTSEN Downloader GUI, and you can begin the data scraping process.
