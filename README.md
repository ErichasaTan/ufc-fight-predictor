# 🥋 UFC Fight Predictor — Machine Learning Pipeline

A full end-to-end machine learning project that predicts the winner of UFC fights using scraped UFCStats data, engineered fight features, and optional betting odds.
This project includes:

- A custom data scraper for UFCStats (fighters, events, fights, and detailed striking/grappling metrics)
- A feature engineering pipeline that transforms raw stats into predictive features
- Multiple machine learning models (Logistic Regression, Random Forest, Gradient Boosting, XGBoost)
- A future-fight prediction pipeline (in progress) that incorporates live betting odds
- Clean project organization with reproducible notebooks and modular Python scripts

## 🚀 Project Goals

1. Build a reliable dataset of UFC fighter attributes, fight history, and event outcomes.

2. Engineer predictive features that capture physical differences, striking style, grappling efficiency, recent form, and more.

3. Train ML models to classify winners and evaluate which features matter most.

4. Scrape live odds for upcoming events and generate predictions for future fight cards.

5. Eventually: Deploy an interactive UI/API so anyone can select an event and view model predictions.

## 📦 Project Structure
```
ufc-fight-predictor/
│
├── src/
│   ├── scraping/
│   │   ├── scrape_ufcstats.py      # Fight + fighter scraper
│   │   ├── scrape_odds.py          # Odds scraper (in progress)
│   │   └── utils/
│   │
│   ├── features/
│   │   ├── build_features.py
│   │
│   ├── models/
│   │   ├── train_model.py
│   │   ├── evaluate.py
│   │   └── predict.py
│   │
│   └── notebooks/
│       ├── 01_data_scraping.ipynb
│       ├── 02_feature_engineering.ipynb
│       ├── 03_model_training.ipynb
│       └── 04_predict_upcoming_fights.ipynb
│
├── data/
│   ├── raw/
│   └── processed/
│
└── README.md
```

## 📊 Data Sources
### UFCStats.com

Used for:

- Fighter profiles
- Event schedules + dates
- Fight results
- Striking/grappling totals
- Significant strike numbers
- Career averages (SLpM, SApM, TD Acc, TD Def, etc.)

### Sportsbook Odds (future integration)

A lightweight scraper will pull the latest betting odds for upcoming UFC cards to improve future fight predictions.

## 🛠️ Feature Engineering

### This project builds a diverse set of predictive features.

#### Physical attributes

- Height difference
- Reach difference
- Age difference (computed using fight date)

#### Striking metrics

SLpM difference
SApM difference
Offensive striking score
Damage margin

#### Grappling metrics

- Takedown average difference
- Takedown accuracy/defense difference
- Submissions per 15 minutes
- Grappling index

#### Recent form

- Number of fights prior to event
- Win rate in last 3 and last 5 fights
- Days since last fight

#### Size-adjusted metrics

- SLpM per pound
- Takedown attempts per pound
- Weight difference in pounds

These help the model generalize better across weight classes and styles.

## 🧪 How to Run

### Install dependencies
``` pip install -r requirements.txt ```
### Scrape UFC data (I recommend using the given CSV as scraping takes a while)
``` python -m src.scraping.scrape_ufcstats```
### Train models
``` python -m src.models.train_model ```
### Predict upcoming fights (after odds scraper is added)
``` python -m src.models.predict ```
