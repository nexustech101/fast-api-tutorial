from setuptools import setup, find_packages

setup(
    name="fastapi-data-pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.104.1",
        "uvicorn>=0.24.0",
        "pydantic>=2.4.2",
        "python-jose[cryptography]>=3.3.0",
        "passlib[bcrypt]>=1.7.4",
        "python-multipart>=0.0.6",
        "sqlalchemy>=2.0.23",
        "alembic>=1.12.1",
        "python-dotenv>=1.0.0",
        "prometheus-client>=0.19.0",
        "pandas>=2.0.3",
        "numpy>=1.24.3",
        "pyarrow>=14.0.1",
        "aiohttp>=3.9.1",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.2",
        "loguru>=0.7.2",
    ],
    extras_require={
        "test": [
            "pytest>=7.4.3",
            "pytest-asyncio>=0.23.2",
            "pytest-cov>=4.1.0",
            "httpx>=0.25.1",
        ],
        "dev": [
            "black>=23.12.1",
            "isort>=5.13.2",
            "flake8>=7.0.0",
            "jupyter>=1.0.0",
        ],
    },
    python_requires=">=3.8",
)
