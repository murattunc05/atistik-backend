
def calculate_winning_probability(horses):
    """
    Calculates the winning probability for a list of horses based on their stats.
    
    Args:
        horses (list): A list of dictionaries, where each dictionary contains horse details.
                       Expected keys: 'races' (list of race results), 'earnings' (str or float), 'age' (str or int)
                       
    Returns:
        list: The input list with an added 'winningProbability' field for each horse.
    """
    if not horses:
        return []

    scores = []
    
    for horse in horses:
        score = 0
        
        # 1. Win Rate & Place Rate
        races = horse.get('races', [])
        total_races = len(races)
        wins = 0
        places = 0 # Top 4
        
        if total_races > 0:
            for race in races:
                try:
                    pos = int(race.get('position', '99').replace('.', ''))
                    if pos == 1:
                        wins += 1
                    if pos <= 4:
                        places += 1
                except (ValueError, AttributeError):
                    pass
            
            win_rate = wins / total_races
            place_rate = places / total_races
            
            score += win_rate * 40  # 40% weight for win rate
            score += place_rate * 20 # 20% weight for place rate
        
        # 2. Recent Form (Last 5 races)
        recent_races = races[:5]
        recent_score = 0
        if recent_races:
            for i, race in enumerate(recent_races):
                try:
                    pos = int(race.get('position', '99').replace('.', ''))
                    # Higher score for better position, weighted by recency
                    # 1st: 10, 2nd: 8, 3rd: 6, 4th: 4, others: 1
                    pos_score = 0
                    if pos == 1: pos_score = 10
                    elif pos == 2: pos_score = 8
                    elif pos == 3: pos_score = 6
                    elif pos == 4: pos_score = 4
                    else: pos_score = 1
                    
                    # Weight: Most recent has highest weight
                    weight = (5 - i) / 15 # 5+4+3+2+1 = 15
                    recent_score += pos_score * weight
                except (ValueError, AttributeError):
                    pass
            
            score += (recent_score / 10) * 25 # 25% weight for recent form (normalized to 0-1 approx)

        # 3. Earnings (Normalized)
        # This is tricky without knowing the max earnings in the set, so we'll do it relative to the group later
        # For now, store the raw earnings to normalize later
        try:
            earnings_str = str(horse.get('prize', '0')).replace('.', '').replace(',', '').replace(' t', '').strip()
            earnings = float(earnings_str)
        except (ValueError, AttributeError):
            earnings = 0
        
        horse['_raw_earnings'] = earnings
        horse['_raw_score'] = score
        scores.append(score)

    # Normalize Earnings
    max_earnings = max([h.get('_raw_earnings', 0) for h in horses]) if horses else 1
    if max_earnings == 0: max_earnings = 1
    
    for horse in horses:
        earnings_score = horse.get('_raw_earnings', 0) / max_earnings
        horse['_raw_score'] += earnings_score * 15 # 15% weight for earnings
    
    # Calculate Probabilities
    total_score = sum([h.get('_raw_score', 0) for h in horses])
    if total_score == 0:
        # Equal probability if no scores
        prob = 100 / len(horses)
        for horse in horses:
            horse['winningProbability'] = round(prob, 1)
    else:
        for horse in horses:
            prob = (horse.get('_raw_score', 0) / total_score) * 100
            horse['winningProbability'] = round(prob, 1)
            
            # Cleanup temporary keys
            if '_raw_earnings' in horse: del horse['_raw_earnings']
            if '_raw_score' in horse: del horse['_raw_score']

    return horses
