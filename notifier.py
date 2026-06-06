import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- SECURE CREDENTIAL CONFIGURATION ---
# --- SECURE CREDENTIAL CONFIGURATION ---
SENDER_EMAIL = "gajaysqldba@gmail.com"
SENDER_PASSWORD = "njwkbfileqjhwzfcw"  # Spaces removed for clean string parsing
RECEIVER_EMAIL = "gajaysqldba@gmail.com"

def send_pipeline_alert(subject, body):
    """Connects securely to the SMTP gateway to dispatch execution alerts."""
    try:
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Open an atomic network context tunnel to the mail gateway
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()  # Encrypt the payload stream
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        print("📨 Notification alert successfully dispatched to inbox.")
    except Exception as e:
        print(f"❌ Alert system internal failure: {e}")