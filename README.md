# 🎰 FTS Assistant Discord Bot

> **A comprehensive Discord bot for managing gambling community leaderboards, automated cryptocurrency rewards, and real-time casino statistics integration with Roobet.**

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Discord.py](https://img.shields.io/badge/discord.py-2.3.2-blue.svg)](https://discordpy.readthedocs.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791.svg)](https://postgresql.org)
[![Roobet](https://img.shields.io/badge/Roobet-API%20Integration-green.svg)](https://roobet.com)

---

## 📋 Table of Contents

- [🎯 Overview](#-overview)
- [✨ Key Features](#-key-features)
- [🏗️ Architecture](#️-architecture)
- [🎮 Bot Commands](#-bot-commands)
- [📊 Leaderboard Systems](#-leaderboard-systems)
- [🎯 Milestone Rewards](#-milestone-rewards)
- [🎲 Slot Challenges](#-slot-challenges)
- [📈 Data Analytics](#-data-analytics)
- [🗄️ Database Schema](#️-database-schema)
- [⚙️ Setup & Installation](#️-setup--installation)
- [🔧 Configuration](#-configuration)
- [📡 API Integration](#-api-integration)
- [🚀 Deployment](#-deployment)

---

## 🎯 Overview

FTS Assistant is a **professional-grade Discord bot** designed for gambling communities, specifically integrated with **Roobet Casino**. It provides real-time leaderboards, automated cryptocurrency rewards, slot challenges, and comprehensive analytics for community engagement.

### 🌟 Highlights

- **Real-time Integration** with Roobet Affiliate & Tipping APIs
- **Automated Cryptocurrency Rewards** ($1 - $1,080+ per milestone)
- **Monthly Prize Pools** ($1,540 USD total in competitions)
- **Live Data Export** (6 JSON files updated every 10 minutes)
- **30-Tier Progression System** with custom Discord emojis
- **Multi-Challenge Support** (up to 10 concurrent slot challenges)

---

## ✨ Key Features

| Feature Category | Description | Value |
|------------------|-------------|--------|
| **🏆 Monthly Leaderboard** | Top 10 weighted wager rankings | **$1,500 USD** prize pool |
| **🎰 Multiplier Board** | Highest single multiplier hits | **$40 USD** weekly prize pool |
| **🎯 Milestone System** | 30-tier progression rewards | **$1-$1,080** per achievement |
| **🎲 Slot Challenges** | Game-specific competitions | **Custom prizes** |
| **📊 Analytics** | Week-to-week trend analysis | **Historical tracking** |
| **🤖 Automation** | Real-time monitoring & payouts | **10-minute intervals** |

---

## 🏗️ Architecture

### Core Components

```
📦 FTS Assistant
├── 🤖 bot.py                 # Main bot framework
├── 🗄️ db.py                  # Database operations
├── 🔌 utils.py               # API integrations
├── 📁 cogs/                  # Modular command systems
│   ├── 👤 user.py            # User commands
│   ├── 🏆 leaderboard.py     # Main leaderboard
│   ├── 🎰 MultiLeaderboard.py # Multiplier rankings
│   ├── 🎯 milestones.py      # Reward system
│   ├── 🎲 slotchallenge.py   # Challenge system
│   ├── ⚙️ admin.py           # Admin controls
│   └── 📊 datamanager.py     # Centralized data
├── 🐘 setup_*.sql           # Database setup
└── 📋 requirements.txt      # Dependencies
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Backend** | Python 3.9+ | Core bot logic |
| **Discord** | discord.py 2.3.2 | Bot framework |
| **Database** | PostgreSQL | Data persistence |
| **APIs** | Roobet Affiliate/Tipping | Live data integration |
| **Hosting** | Heroku | Cloud deployment |
| **Storage** | GitHub | JSON data export |

---

## 🎮 Bot Commands

### 👤 User Commands

| Command | Description | Access |
|---------|-------------|--------|
| `/mywager <username>` | Check personal wager statistics | Public |
| `/monthlygoal` | View community monthly targets | Public |
| `/monthtomonth` | Generate wager trend charts | Public |
| `/lifetimestats` | Current month totals | Public |

### ⚙️ Admin Commands

| Command | Description | Access |
|---------|-------------|--------|
| `/tipuser <username> <amount>` | Send manual cryptocurrency tip | Admin Only |
| `/tipstats` | View tipping statistics | Admin Only |
| `/setchallenge` | Create new slot challenge | Admin Only |
| `/cancelchallenge <id>` | Remove active challenge | Admin Only |
| `/clear_tips` | Reset milestone database | Admin Only |
| `/status` | Check bot health | Admin Only |

### 🔍 Challenge Commands

| Command | Description | Access |
|---------|-------------|--------|
| `/challenge_results` | View active challenge leaderboards | Public |
| `/gamestats <username> <identifier>` | Game-specific user statistics (current month) | Public |

---

## 📊 Leaderboard Systems

### 🏆 Monthly Leaderboard

**Prize Distribution**: $1,500 USD Total

| Rank | Prize | Requirements |
|------|-------|-------------|
| 🥇 #1 | **$500** | Highest weighted wager |
| 🥈 #2 | **$300** | Second highest |
| 🥉 #3 | **$225** | Third highest |
| #4 | **$175** | Fourth highest |
| #5 | **$125** | Fifth highest |
| #6-10 | **$75-$5** | Remaining top 10 |

#### Weighted Wager Rules
- **≤97% RTP Games**: 100% contribution
- **>97% RTP Games**: 50% contribution  
- **≥98% RTP Games**: 10% contribution
- **Slots & House Games Only** (Dice excluded)

### 🎰 Multiplier Leaderboard

**Prize Distribution**: $40 USD Weekly / $160 USD Monthly

| Rank | Weekly Prize | Criteria |
|------|-------|----------|
| 🥇 #1 | **$20** | Highest single multiplier |
| 🥈 #2 | **$15** | Second highest |
| 🥉 #3 | **$5** | Third highest |

**Competition Period**: Monday 12:00 AM UTC to Sunday 11:59 PM UTC

---

## 🎯 Milestone Rewards

### 30-Tier Progression System

| Tier Range | Wager Requirement | Tip Amount | Color Theme |
|------------|------------------|------------|-------------|
| **Ranks 1-5** | $50 - $400 | **$1.00** | 🟡 Gold |
| **Ranks 6-8** | $600 - $1,000 | **$2.00** | ⚪ Silver |
| **Ranks 9-12** | $1,500 - $3,000 | **$3.00** | 🟤 Bronze |
| **Ranks 13-15** | $5,000 - $10,000 | **$11-$14** | 🟢 Green |
| **Ranks 16-18** | $15,000 - $25,000 | **$27.00** | 🔵 Blue |
| **Ranks 19-20** | $35,000 - $50,000 | **$54-$81** | 🟣 Purple |
| **Ranks 21-25** | $75,000 - $250,000 | **$135-$270** | 🔴 Red/Orange |
| **Ranks 26-30** | $350,000 - $1,000,000 | **$540-$1,080** | 🔥 Elite |

### Key Features
- ✅ **Automatic Detection**: Real-time progress monitoring
- 💰 **Instant Payouts**: Cryptocurrency tips sent immediately
- 🎨 **Custom Emojis**: Unique Discord emojis for each rank
- 🛡️ **Duplicate Prevention**: Smart database tracking
- 📊 **Progress Tracking**: Cumulative reward calculations

---

## 🎲 Slot Challenges

### Challenge System

| Feature | Description |
|---------|-------------|
| **Concurrent Challenges** | Up to 10 active simultaneously |
| **Game Targeting** | Specific Roobet slot games |
| **Custom Requirements** | Multiplier thresholds, minimum bets |
| **Automatic Detection** | 10-minute API polling |
| **Instant Payouts** | Cryptocurrency rewards on completion |

### Challenge Configuration

```yaml
Challenge Parameters:
  - Game Identifier: (e.g., "pragmatic:vs10bbbbrnd")
  - Game Display Name: (e.g., "Big Bass Bonanza")
  - Required Multiplier: (e.g., 100x)
  - Prize Amount: (USD)
  - Minimum Bet: (Optional, USD)
  - Custom Emoji: (Optional)
```

### Management Features
- 🎯 **Live Monitoring**: Real-time winner detection
- 🏆 **Automatic Payouts**: Instant cryptocurrency rewards
- 📋 **Challenge History**: Complete archive of results
- ⚙️ **Admin Controls**: Create, cancel, modify challenges
- 🔗 **Game Integration**: Direct links to Roobet games

---

## 📈 Data Analytics

### 📊 Real-time Statistics
- **Current Month Totals**: Live wager tracking
- **Month-to-Month Trends**: 12-month rolling analysis
- **Projection Analytics**: Month-end estimates
- **User Lookup**: Individual statistics
- **Community Goals**: Milestone celebrations

### 📋 Export System

| File | Content | Update Frequency |
|------|---------|------------------|
| `latestLBResults.json` | Main leaderboard data | 10 minutes |
| `LatestMultiLBResults.json` | Multiplier rankings | 10 minutes |
| `ActiveSlotChallenges.json` | Current challenges | 10 minutes |
| `allTimeTips.json` | Complete tip history | 10 minutes |
| `challengeHistory.json` | Challenge archive | 10 minutes |
| `allWagerData.json` | Comprehensive statistics | 10 minutes |

---

## 🗄️ Database Schema

### Core Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| **milestonetips** | Milestone tracking | `user_id`, `tier`, `month`, `year` |
| **manualtips** | All tip transactions | `user_id`, `amount`, `tip_type`, `tipped_at` |
| **active_slot_challenges** | Live challenges | `challenge_id`, `game_identifier`, `required_multi` |
| **slot_challenge_logs** | Challenge history | `winner_uid`, `multiplier`, `prize`, `challenge_start` |
| **monthly_totals** | Historical aggregates | `year`, `month`, `total_wager`, `weighted_wager` |
| **settings** | Configuration | `key`, `value` |

### Data Flow

```mermaid
graph TD
    A[Roobet APIs] --> B[DataManager]
    B --> C[Database]
    B --> D[JSON Export]
    C --> E[Bot Commands]
    C --> F[Leaderboards]
    C --> G[Milestones]
    C --> H[Challenges]
```

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.9+
- PostgreSQL Database
- Discord Bot Token
- Roobet API Credentials

### Installation Steps

1. **Clone Repository**
   ```bash
   git clone https://github.com/FTSStreams/fts-assistant.git
   cd fts-assistant
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Database Setup**
   ```bash
   # Run SQL setup files in your PostgreSQL client
   psql -f setup_monthly_totals.sql
   psql -f add_total_wager_column.sql
   ```

4. **Environment Configuration**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

5. **Start Bot**
   ```bash
   python bot.py
   ```

---

## 🔧 Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DISCORD_TOKEN` | Bot authentication token | `your_discord_token` |
| `DATABASE_URL` | PostgreSQL connection | `postgresql://user:pass@host:5432/db` |
| `ROOBET_API_TOKEN` | Affiliate API access | `your_roobet_affiliate_token` |
| `TIPPING_API_TOKEN` | Tipping API access | `your_roobet_tipping_token` |
| `ROOBET_USER_ID` | Bot's Roobet account ID | `12345678` |
| `GITHUB_TOKEN` | Data export access | `your_github_token` |
| `GUILD_ID` | Discord server ID | `1234567890` |
| `LEADERBOARD_CHANNEL_ID` | Main leaderboard channel | `1234567890` |
| `CHALLENGE_CHANNEL_ID` | Challenge announcements | `1234567890` |
| `MILESTONE_CHANNEL_ID` | Milestone celebrations | `1234567890` |
| `WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID` | Weekly multiplier payout logs | `1234567890` |

### Channel Configuration

```yaml
Required Discord Channels:
  - Leaderboard Display: Monthly rankings
  - Multiplier Leaderboard: Top multipliers
  - Milestone Celebrations: Achievement announcements  
  - Challenge Announcements: Active challenges
  - Challenge Logs: Winner notifications
  - Monthly Goals: Community milestones
  - Weekly Multiplier Logs: Weekly payout announcements
```

---

## 📡 API Integration

### Roobet Affiliate API
- **Endpoint**: `https://roobetconnect.com/affiliate/v2/stats`
- **Data**: User wager statistics, multiplier records
- **Rate Limits**: Managed with retry logic
- **Authentication**: Bearer token

### Roobet Tipping API
- **Endpoint**: `https://roobet.com/_api/tipping/send`
- **Function**: Automated cryptocurrency tips
- **Security**: Nonce-based request signing
- **Limits**: Intelligent queuing system

### GitHub Data Export
- **Repository**: Automated JSON file uploads
- **Frequency**: Every 10 minutes
- **Format**: RESTful API-ready JSON structures
- **Access**: Public data endpoints for web integration

---

## 🚀 Deployment

### Heroku Deployment

1. **Heroku Setup**
   ```bash
   heroku create your-bot-name
   heroku addons:create heroku-postgresql:hobby-dev
   ```

2. **Environment Variables**
   ```bash
   heroku config:set DISCORD_TOKEN=your_token
   heroku config:set ROOBET_API_TOKEN=your_api_token
   # ... set all required variables
   ```

3. **Deploy**
   ```bash
   git push heroku main
   ```

### Process Configuration (`Procfile`)
```
worker: python bot.py
```

### Auto-deployment
- Configured for automatic Git pushes via `auto-git-push.bat`
- Heroku integration for seamless updates
- Environment-based configuration management

---

## 📊 Monitoring & Analytics

### Performance Metrics
- **API Response Times**: Monitored and logged
- **Database Performance**: Connection pooling optimization
- **Error Rates**: Comprehensive logging and alerting
- **User Engagement**: Command usage statistics

### Logging System
- **File Logging**: Persistent log storage (`bot.log`)
- **Console Output**: Real-time monitoring
- **Error Tracking**: Detailed exception handling
- **Performance Metrics**: API call timing and success rates

---

## 🛡️ Security & Privacy

### Data Protection
- **Username Masking**: Public displays show `user***` format
- **Input Validation**: Comprehensive sanitization
- **Rate Limiting**: API abuse prevention
- **Access Controls**: Admin-only sensitive operations

### Security Measures
- **Environment Variables**: Secure credential storage
- **Database Security**: Connection pooling and prepared statements
- **API Authentication**: Token-based security
- **Error Handling**: No sensitive data in error messages

---

## 📝 License & Contributing

This project is proprietary software for FTSStreams gambling community management.

### Support
For technical support or feature requests, contact the development team through Discord.

---

## 🏆 Achievement Stats

Since January 1st, 2025:
- **Tips Distributed**: $11,295.53+ USD
- **Users Engaged**: 1000+ community members
- **Challenges Completed**: 50+ slot competitions
- **Weekly Prizes**: $1,540 USD distributed monthly

---

*Built with ❤️ for the FTSStreams gambling community*

**Last Updated**: November 16, 2025