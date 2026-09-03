"""
Generate realistic sample ticket data for testing
"""
#Environment Setup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration making tickets
NUM_TICKETS = 500  # Start with 500, can increase later
START_DATE = datetime.now() - timedelta(days=180)  # Last 6 months
END_DATE = datetime.now()

# Dictionary of categories and their common issues
CATEGORIES = {
    'Billing': [
        'charged twice for subscription',
        'payment method declined',
        'unexpected charge on credit card',
        'refund not received',
        'invoice incorrect amount',
        'subscription auto-renewed unwanted',
        'promo code not applied',
        'tax calculation wrong'
    ],
    'Technical': [
        'app crashes on startup',
        'cannot login to account',
        'features not loading properly',
        'slow performance issues',
        'error message when saving',
        'mobile app not syncing',
        'integration not working',
        'API returning errors'
    ],
    'Delivery': [
        'package not delivered on time',
        'wrong item received',
        'damaged product arrived',
        'tracking number not working',
        'delivery to wrong address',
        'missing items from order',
        'package marked delivered but not received'
    ],
    'Account': [
        'cannot reset password',
        'account locked after failed login',
        'email not receiving notifications',
        'profile information not updating',
        'two-factor authentication issues',
        'cannot delete account',
        'want to change email address'
    ],
    'Product': [
        'product defective out of box',
        'missing parts in package',
        'product not as described',
        'quality issues with material',
        'instructions unclear or missing',
        'warranty claim process',
        'replacement part needed'
    ],
    'Refund': [
        'want to return product',
        'refund taking too long',
        'partial refund received',
        'return label not working',
        'refund to wrong payment method',
        'restocking fee too high'
    ]
}
# more descriptive lists
CHANNELS = ['Email', 'Chat', 'Phone', 'Web Form']
PRIORITIES = ['Low', 'Medium', 'High', 'Critical']
CUSTOMER_TIERS = ['Free', 'Basic', 'Premium', 'Enterprise']
PRODUCTS = ['Product A', 'Product B', 'Product C', 'Product D', 'Service X', 'Service Y']

# Severity keywords for later scoring
URGENT_WORDS = ['urgent', 'critical', 'immediately', 'asap', 'emergency', 'broken', 'not working']
FRUSTRATED_WORDS = ['frustrated', 'angry', 'disappointed', 'unacceptable', 'terrible', 'awful']
# function 
def generate_ticket_text(category, issue):
    """Generate realistic ticket text"""
    
    # Opening phrases
    openings = [
        "Hi, I need help with",
        "Hello, I'm having an issue with",
        "I'm experiencing a problem with",
        "Can someone help me with",
        "I need assistance with",
        "There's an issue with",
        "I'm writing because"
    ]
    
    # Add context
    contexts = [
        "This has been happening for the past few days.",
        "I've tried multiple times but no luck.",
        "This is affecting my work.",
        "I need this resolved soon.",
        "Can you please look into this?",
        "I've already contacted support before about this.",
        "This is the second time this has happened."
    ]
    
    # Randomly add urgency or frustration
    emotion = ""
    if random.random() < 0.3:  # 30% chance
        if random.random() < 0.5:
            emotion = f" This is {random.choice(URGENT_WORDS)}!"
        else:
            emotion = f" I'm really {random.choice(FRUSTRATED_WORDS)} about this."
    
    # Build ticket text
    opening = random.choice(openings)
    context = random.choice(contexts)
    
    text = f"{opening} {issue}. {context}{emotion}"
    
    return text

def generate_tickets():
    """Generate sample ticket dataset"""
    
    tickets = []
    
    for i in range(NUM_TICKETS):
        # Generate random timestamp
        time_delta = (END_DATE - START_DATE).total_seconds()
        random_seconds = random.uniform(0, time_delta)
        created_at = START_DATE + timedelta(seconds=random_seconds)
        
        # Pick category and issue
        category = random.choice(list(CATEGORIES.keys()))
        issue = random.choice(CATEGORIES[category])
        
        # Generate ticket text
        text = generate_ticket_text(category, issue)
        
        # Other fields
        ticket = {
            'ticket_id': f'TKT-{100000 + i}',
            'customer_id': f'CUST-{random.randint(1000, 9999)}',
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'text': text,
            'category': category,
            'channel': random.choice(CHANNELS),
            'priority': random.choice(PRIORITIES),
            'customer_tier': random.choice(CUSTOMER_TIERS),
            'product': random.choice(PRODUCTS)
        }
        
        tickets.append(ticket)
    
    # Create DataFrame
    df = pd.DataFrame(tickets)
    
    # Sort by created_at
    df = df.sort_values('created_at').reset_index(drop=True)
    
    return df

def main():
    print("=" * 60)
    print("GENERATING SAMPLE TICKET DATA")
    print("=" * 60)
    
    # Generate data
    print(f"\nGenerating {NUM_TICKETS} tickets...")
    df = generate_tickets()
    
    # Display statistics
    print("\n✅ Data generated successfully!")
    print(f"\nDataset shape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nDate range: {df['created_at'].min()} to {df['created_at'].max()}")
    print(f"\nCategories distribution:")
    print(df['category'].value_counts())
    
    # Save to CSV
    output_path = 'data/samples/tickets_sample.csv'
    df.to_csv(output_path, index=False)
    print(f"\n✅ Saved to: {output_path}")
    
    # Show sample
    print("\n📋 Sample tickets:")
    print(df.head(3)[['ticket_id', 'category', 'text']])
    
    print("\n" + "=" * 60)
    print("✅ SAMPLE DATA GENERATION COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    main()