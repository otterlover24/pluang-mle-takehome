from setuptools import setup, find_packages

setup(
    name="crypto_research",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "langchain>=0.1.0",
        "langchain-openai>=0.0.2",
        "langgraph>=0.0.20",
        "python-dotenv>=0.21.0",
        "pandas>=2.0.0",
        "python-dateutil>=2.8.2",
        "typer>=0.9.0",
        "rich>=13.0.0",
        "reportlab>=3.6.0",
        "requests>=2.28.0",
        "beautifulsoup4>=4.11.0",
        "questionary>=2.0.1",
        "markdown>=3.5.0"
    ],
    entry_points={
        "console_scripts": [
            "crypto_research=crypto_research.cli.main:app",
        ],
    },
)