import discord

# Single source of truth for milestone thresholds, tip amounts, and display metadata.
# New 45-rank system: Bronze (1-15), Silver (16-30), Gold (31-45)
MILESTONES = [
    # Bronze Tier (1-15)
    {"tier": "Rank 1", "threshold": 50, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b1:1538739895470792834>"},
    {"tier": "Rank 2", "threshold": 100, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b2:1538739920435159040>"},
    {"tier": "Rank 3", "threshold": 150, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b3:1538739946230259742>"},
    {"tier": "Rank 4", "threshold": 250, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b4:1538739964391727194>"},
    {"tier": "Rank 5", "threshold": 400, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b5:1538739982938673294>"},
    {"tier": "Rank 6", "threshold": 600, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b6:1538739999103520848>"},
    {"tier": "Rank 7", "threshold": 800, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b7:1538740016258351164>"},
    {"tier": "Rank 8", "threshold": 1000, "tip": 1.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b8:1538740037355577425>"},
    {"tier": "Rank 9", "threshold": 1500, "tip": 2.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b9:1538740055886266468>"},
    {"tier": "Rank 10", "threshold": 2000, "tip": 2.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b10:1538740073992814702>"},
    {"tier": "Rank 11", "threshold": 2500, "tip": 2.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b11:1538742477522075668>"},
    {"tier": "Rank 12", "threshold": 3000, "tip": 2.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b12:1538742528348651520>"},
    {"tier": "Rank 13", "threshold": 5000, "tip": 8.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b13:1538742558447116369>"},
    {"tier": "Rank 14", "threshold": 7500, "tip": 10.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b14:1538742575010287738>"},
    {"tier": "Rank 15", "threshold": 10000, "tip": 10.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:b15:1538742595700654260>"},
    
    # Silver Tier (16-30)
    {"tier": "Rank 16", "threshold": 15000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s1:1538744672720261180>"},
    {"tier": "Rank 17", "threshold": 20000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s2:1538744732920840222>"},
    {"tier": "Rank 18", "threshold": 25000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s3:1538744750113292329>"},
    {"tier": "Rank 19", "threshold": 30000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s4:1538744767297355850>"},
    {"tier": "Rank 20", "threshold": 35000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s5:1538744784590606347>"},
    {"tier": "Rank 21", "threshold": 40000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s6:1538744801925537792>"},
    {"tier": "Rank 22", "threshold": 45000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s7:1538744819197681694>"},
    {"tier": "Rank 23", "threshold": 50000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s8:1538744840660066406>"},
    {"tier": "Rank 24", "threshold": 55000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s9:1538744855000256604>"},
    {"tier": "Rank 25", "threshold": 60000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s10:1538744872352092292>"},
    {"tier": "Rank 26", "threshold": 65000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s11:1538744889095749712>"},
    {"tier": "Rank 27", "threshold": 70000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s12:1538744906162511962>"},
    {"tier": "Rank 28", "threshold": 75000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s13:1538744919781285889>"},
    {"tier": "Rank 29", "threshold": 80000, "tip": 20.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s14:1538744934646022175>"},
    {"tier": "Rank 30", "threshold": 90000, "tip": 40.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:s15:1538744950483722290>"},
    
    # Gold Tier (31-45)
    {"tier": "Rank 31", "threshold": 100000, "tip": 40.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g1:1538748212188545024>"},
    {"tier": "Rank 32", "threshold": 125000, "tip": 100.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g2:1538748236943200376>"},
    {"tier": "Rank 33", "threshold": 150000, "tip": 100.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g3:1538748255419109376>"},
    {"tier": "Rank 34", "threshold": 175000, "tip": 100.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g4:1538748270887829695>"},
    {"tier": "Rank 35", "threshold": 200000, "tip": 100.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g5:1538748287056875550>"},
    {"tier": "Rank 36", "threshold": 250000, "tip": 200.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g6:1538748302747639869>"},
    {"tier": "Rank 37", "threshold": 300000, "tip": 200.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g7:1538748318396448861>"},
    {"tier": "Rank 38", "threshold": 350000, "tip": 200.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g8:1538748331218706502>"},
    {"tier": "Rank 39", "threshold": 400000, "tip": 200.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g9:1538748345244327937>"},
    {"tier": "Rank 40", "threshold": 500000, "tip": 400.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g10:1538748362642300959>"},
    {"tier": "Rank 41", "threshold": 600000, "tip": 400.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g11:1538748375858413679>"},
    {"tier": "Rank 42", "threshold": 700000, "tip": 400.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g12:1538748394997153852>"},
    {"tier": "Rank 43", "threshold": 800000, "tip": 400.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g13:1538748410172022854>"},
    {"tier": "Rank 44", "threshold": 900000, "tip": 400.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g14:1538748423862354000>"},
    {"tier": "Rank 45", "threshold": 1000000, "tip": 400.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:g15:1538748437565014027>"}
]