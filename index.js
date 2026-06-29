import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import multer from 'multer';
import makeWASocket, { useMultiFileAuthState, makeCacheableSignalKeyStore, DisconnectReason, Browsers, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { Boom } from '@hapi/boom';
import cors from 'cors';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 8000;
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

const SESSION_FILE = './running_sessions.json';
const userSessions = {};
const stopFlags = {};
const activeSockets = {};
const messageQueues = {};
const reconnectAttempts = {};

const saveSessions = () => {
  try {
    fs.writeFileSync(SESSION_FILE, JSON.stringify(userSessions, null, 2), 'utf8');
  } catch (error) {
    console.error(chalk.red(`Error saving sessions: ${error.message}`));
  }
};

const generateUniqueKey = () => {
  return crypto.randomBytes(16).toString('hex'); 
};

const EXPIRY_TIME = Infinity;
const checkSessionExpiry = (sessionTimestamp, sessionMeta) => {
  if (sessionMeta?.neverExpire) return false;
  return (Date.now() - sessionTimestamp) > EXPIRY_TIME;
};

const deleteExpiredSessions = () => {
  try {
    for (const userId in userSessions) {
      const { uniqueKey, lastUpdateTimestamp } = userSessions[userId];
      if (checkSessionExpiry(lastUpdateTimestamp)) {
        const sessionPath = `./session/${uniqueKey}`;
        if (fs.existsSync(sessionPath)) {
          try {
            fs.rmdirSync(sessionPath, { recursive: true });
          } catch (err) {}
        }
        delete userSessions[userId];
        saveSessions();
      }
    }
  } catch (err) {}
};

const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  // Initialize message queue for this session
  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = {
      messages: [...messages],
      currentIndex: 0,
      isSending: false
    };
  }

  const queue = messageQueues[uniqueKey];
  
  const sendNextMessage = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete messageQueues[uniqueKey];
      return;
    }

    // Check if socket is still active
    if (!activeSockets[uniqueKey]) {
      console.log(chalk.yellow(`⚠️ Socket disconnected for ${uniqueKey}, waiting for reconnection...`));
      return;
    }

    if (queue.isSending) {
      return; // Skip if already sending
    }

    if (queue.messages.length === 0) {
      console.log(chalk.yellow(`No messages to send for ${uniqueKey}`));
      return;
    }

    queue.isSending = true;
    const chatId = target.includes('@g.us') ? target : `${target}@s.whatsapp.net`;
    const currentMessage = queue.messages[queue.currentIndex];
    const formattedMessage = `${hatersName} ${currentMessage}`;

    try {
      await MznKing.sendMessage(chatId, { text: formattedMessage });
      console.log(chalk.green(`✉️ Message sent [${queue.currentIndex + 1}/${queue.messages.length}]: ${formattedMessage.substring(0, 50)}...`));
      
      // Move to next message
      queue.currentIndex++;
      
      // If all messages sent, restart from beginning
      if (queue.currentIndex >= queue.messages.length) {
        console.log(chalk.cyan(`🔄 All messages sent! Restarting from first message...`));
        queue.currentIndex = 0;
      }
    } catch (err) {
      console.error(chalk.red(`❌ Error sending message: ${err.message}`));
      // Don't stop on error, continue with next message
    } finally {
      queue.isSending = false;
    }
  };

  const interval = parseInt(speed) * 1000;
  const messageInterval = setInterval(sendNextMessage, interval);
  stopFlags[uniqueKey] = { stopped: false, interval: messageInterval };
  console.log(chalk.cyan(`📨 Message automation started! Sending every ${speed} seconds`));
  
  // Send first message immediately
  sendNextMessage();
};

const connectAndLogin = async (phoneNumber, uniqueKey, sendPairingCode = null) => {
  const sessionPath = `./session/${uniqueKey}`;
  let pairingCodeSent = false;

  const startConnection = async () => {
    try {
      console.log(chalk.magenta(`🚀 Starting connection for ${phoneNumber}, uniqueKey: ${uniqueKey}`));

      if (!fs.existsSync(sessionPath)) {
        fs.mkdirSync(sessionPath, { recursive: true });
      }

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

      const MznKing = makeWASocket({
        version,
        logger: pino.default({ level: 'silent' }),
        browser: Browsers.windows('Firefox'),
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, pino.default({ level: 'silent' }))
        },
        printQRInTerminal: false,
        generateHighQualityLinkPreview: true,
        markOnlineOnConnect: true,
        getMessage: async () => undefined,
        keepAliveIntervalMs: 30000,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: undefined,
        retryRequestDelayMs: 250,
      });

      activeSockets[uniqueKey] = MznKing;

      // Request pairing code if not registered and callback provided
      if (!MznKing.authState.creds.registered && !pairingCodeSent && sendPairingCode) {
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        try {
          const cleanedNumber = phoneNumber.replace(/[^0-9]/g, '');
          console.log(chalk.cyan(`🔐 Requesting pairing code for ${cleanedNumber}...`));
          
          const code = await MznKing.requestPairingCode(cleanedNumber);
          const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
          
          console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
          console.log(chalk.green(`✅ Pairing Code: ${pairingCode}`));
          console.log(chalk.yellow(`⏳ Waiting for link (2 minutes)...`));
          console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));

          if (!pairingCodeSent) {
            pairingCodeSent = true;
            sendPairingCode(pairingCode, false);
          }
        } catch (error) {
          console.error(chalk.red(`❌ Pairing error: ${error.message}`));
          if (!pairingCodeSent && sendPairingCode) {
            pairingCodeSent = true;
            sendPairingCode(null, false, error.message);
          }
        }
      } else if (MznKing.authState.creds.registered) {
        console.log(chalk.green(`✅ Session already registered for ${uniqueKey}`));
        if (!pairingCodeSent && sendPairingCode) {
          pairingCodeSent = true;
          sendPairingCode(null, true);
        }
      }

      MznKing.ev.on("connection.update", async (update) => {
        const { connection, lastDisconnect } = update;
        
        if (connection === "open") {
          console.log(chalk.green(`\n✅✅✅ WhatsApp Connected! (${uniqueKey}) ✅✅✅\n`));
          reconnectAttempts[uniqueKey] = 0;
          
          userSessions[uniqueKey] = { 
            ...userSessions[uniqueKey],
            phoneNumber, 
            uniqueKey,
            connected: true,
            lastUpdateTimestamp: Date.now() 
          };
          saveSessions();

          if (!pairingCodeSent && sendPairingCode) {
            pairingCodeSent = true;
            sendPairingCode(null, true);
          }

          // Resume messaging if it was active before disconnection
          if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
            const { target, hatersName, messages, speed } = userSessions[uniqueKey];
            console.log(chalk.cyan(`🔄 Resuming message automation for ${uniqueKey}...`));
            
            // Restore message queue state if exists
            if (!messageQueues[uniqueKey]) {
              messageQueues[uniqueKey] = {
                messages: [...messages],
                currentIndex: 0,
                isSending: false
              };
            }
            
            startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
          }
        }

        if (connection === "close") {
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
          
          console.log(chalk.red(`⚠️ Connection closed - Status: ${statusCode}, Reason: ${reason}`));

          // Handle different disconnect reasons
          if (reason === DisconnectReason.badSession) {
            console.log(chalk.red(`Bad Session File, Deleting and Reconnecting...`));
            try {
              fs.rmdirSync(sessionPath, { recursive: true });
            } catch (e) {}
          } else if (reason === DisconnectReason.connectionClosed) {
            console.log(chalk.yellow(`Connection closed, reconnecting...`));
          } else if (reason === DisconnectReason.connectionLost) {
            console.log(chalk.yellow(`Connection lost from server, reconnecting...`));
          } else if (reason === DisconnectReason.connectionReplaced) {
            console.log(chalk.red(`Connection replaced, another new session opened. Stopping...`));
            stopFlags[uniqueKey] = { stopped: true };
            delete activeSockets[uniqueKey];
            return;
          } else if (reason === DisconnectReason.loggedOut) {
            console.log(chalk.red(`Device logged out, deleting session and stopping...`));
            try {
              fs.rmdirSync(sessionPath, { recursive: true });
            } catch (e) {}
            stopFlags[uniqueKey] = { stopped: true };
            delete activeSockets[uniqueKey];
            return;
          } else if (reason === DisconnectReason.restartRequired) {
            console.log(chalk.yellow(`Restart required, restarting...`));
          } else if (reason === DisconnectReason.timedOut) {
            console.log(chalk.yellow(`Connection timed out, reconnecting...`));
          }

          // Reconnect logic with exponential backoff
          if (!stopFlags[uniqueKey]?.stopped && reason !== 401) {
            reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
            const delay = Math.min(3000 * reconnectAttempts[uniqueKey], 30000);
            
            console.log(chalk.yellow(`🔄 Reconnecting in ${delay/1000} seconds... (Attempt ${reconnectAttempts[uniqueKey]})`));
            setTimeout(() => startConnection(), delay);
          }
        }
      });

      MznKing.ev.on('creds.update', saveCreds);
      MznKing.ev.on("messages.upsert", () => {});

    } catch (error) {
      console.error(chalk.red(`❌ ERROR: ${error.message}`));
      if (!pairingCodeSent && sendPairingCode) {
        pairingCodeSent = true;
        sendPairingCode(null, false, error.message);
      }
      if (!stopFlags[uniqueKey]?.stopped) {
        reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
        const delay = Math.min(5000 * reconnectAttempts[uniqueKey], 30000);
        setTimeout(() => startConnection(), delay);
      }
    }
  };

  await startConnection();
};

// Restore sessions on startup
const restoreSessions = async () => {
  if (fs.existsSync(SESSION_FILE)) {
    try {
      const data = fs.readFileSync(SESSION_FILE, 'utf8');
      const savedSessions = JSON.parse(data);
      Object.assign(userSessions, savedSessions);
      
      console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
      console.log(chalk.green(`📂 Found ${Object.keys(userSessions).length} saved sessions`));
      console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));
      
      // Restore each session
      for (const [key, session] of Object.entries(userSessions)) {
        if (session.phoneNumber && session.uniqueKey) {
          const sessionPath = `./session/${session.uniqueKey}`;
          
          // Check if session folder exists
          if (fs.existsSync(sessionPath)) {
            console.log(chalk.cyan(`🔄 Restoring session: ${session.uniqueKey} (${session.phoneNumber})`));
            
            // Initialize stop flag
            stopFlags[session.uniqueKey] = { stopped: false };
            reconnectAttempts[session.uniqueKey] = 0;
            
            // Restore message queue if messaging was active
            if (session.messaging && session.messages) {
              messageQueues[session.uniqueKey] = {
                messages: [...session.messages],
                currentIndex: 0,
                isSending: false
              };
              console.log(chalk.yellow(`📨 Session ${session.uniqueKey} had active messaging - will resume after connection`));
            }
            
            // Reconnect without sending pairing code (already paired)
            await connectAndLogin(session.phoneNumber, session.uniqueKey, null);
            
            // Small delay between connections
            await new Promise(resolve => setTimeout(resolve, 1000));
          } else {
            console.log(chalk.yellow(`⚠️ Session folder not found for ${session.uniqueKey}, skipping...`));
          }
        }
      }
      
      console.log(chalk.green(`\n✅ Session restoration complete!\n`));
    } catch (err) {
      console.error(chalk.red(`Error loading session file: ${err.message}`));
    }
  }
};

// Login endpoint - only requires phone number
app.post('/login', async (req, res) => {
  try {
    let { phoneNumber } = req.body;

    if (!phoneNumber) {
      return res.status(400).json({ success: false, message: 'Phone number is required!' });
    }

    phoneNumber = phoneNumber.replace(/[^0-9]/g, '');
    console.log(chalk.cyan(`📞 Login request for: ${phoneNumber}`));

    const uniqueKey = generateUniqueKey();
    stopFlags[uniqueKey] = { stopped: false };
    reconnectAttempts[uniqueKey] = 0;

    const sendPairingCode = (pairingCode, isConnected = false, errorMsg = null) => {
      if (errorMsg) {
        res.json({
          success: false,
          message: 'Error generating pairing code',
          error: errorMsg,
          uniqueKey: uniqueKey
        });
      } else if (isConnected) {
        res.json({
          success: true,
          message: 'WhatsApp Connected Successfully!',
          connected: true,
          uniqueKey: uniqueKey
        });
      } else {
        res.json({
          success: true,
          message: 'Pairing code generated successfully',
          pairingCode: pairingCode,
          uniqueKey: uniqueKey
        });
      }
    };

    await connectAndLogin(phoneNumber, uniqueKey, sendPairingCode);
  } catch (error) {
    console.error(chalk.red(`Error in /login endpoint: ${error.message}`));
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// Get groups for logged in session
app.post('/getGroupUID', async (req, res) => {
  try {
    const { uniqueKey } = req.body;

    if (!uniqueKey) {
      return res.status(400).json({ success: false, message: 'Missing uniqueKey in request' });
    }

    if (!userSessions[uniqueKey]) {
      return res.status(400).json({ success: false, message: 'Invalid key or no active session found' });
    }

    if (!activeSockets[uniqueKey]) {
      return res.status(400).json({ success: false, message: 'WhatsApp socket not connected' });
    }

    const MznKing = activeSockets[uniqueKey];

    try {
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      const groups = await MznKing.groupFetchAllParticipating();
      
      const groupUIDs = Object.values(groups).map(group => ({
        groupName: group.subject,
        groupId: group.id,
      }));

      console.log(chalk.green(`✅ Fetched ${groupUIDs.length} groups for session ${uniqueKey}`));
      res.json({ success: true, groupUIDs });
    } catch (fetchError) {
      console.error(chalk.red(`Error fetching groups: ${fetchError.message}`));
      return res.status(500).json({ success: false, message: 'Error fetching groups from WhatsApp' });
    }
  } catch (error) {
    console.error(chalk.red(`Unexpected server error: ${error.message}`));
    res.status(500).json({ success: false, message: 'Internal Server Error' });
  }
});

// Start messaging endpoint
app.post('/startMessaging', upload.single('messageFile'), async (req, res) => {
  try {
    const { uniqueKey, target, hatersName, speed } = req.body;
    const filePath = req.file?.path;

    if (!uniqueKey || !target || !hatersName || !speed) {
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    }

    if (!userSessions[uniqueKey]) {
      return res.status(400).json({ success: false, message: 'Invalid session key!' });
    }

    if (!activeSockets[uniqueKey]) {
      return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });
    }

    if (!filePath) {
      return res.status(400).json({ success: false, message: 'No message file uploaded!' });
    }

    let messages = [];
    try {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      messages = fileContent.split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0);
      
      console.log(chalk.green(`📄 Loaded ${messages.length} messages from file`));
      
      if (messages.length === 0) {
        return res.status(400).json({ success: false, message: 'File is empty or contains no valid messages!' });
      }
    } catch (error) {
      console.error(chalk.red(`Error reading file: ${error.message}`));
      return res.status(500).json({ success: false, message: 'Error reading messages file!' });
    } finally {
      try {
        fs.unlinkSync(filePath);
      } catch (e) {}
    }

    const MznKing = activeSockets[uniqueKey];
    
    userSessions[uniqueKey].target = target;
    userSessions[uniqueKey].hatersName = hatersName;
    userSessions[uniqueKey].messages = messages;
    userSessions[uniqueKey].speed = speed;
    userSessions[uniqueKey].messaging = true;
    saveSessions();

    startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);

    res.json({
      success: true,
      message: 'Message automation started successfully!',
      uniqueKey: uniqueKey,
      messageCount: messages.length
    });
  } catch (error) {
    console.error(chalk.red(`Error in /startMessaging endpoint: ${error.message}`));
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// Stop process endpoint
app.post('/stop', async (req, res) => {
  const { uniqueKey } = req.body;
  if (!uniqueKey) {
    return res.status(400).json({ success: false, message: 'Missing uniqueKey in request' });
  }

  if (!userSessions[uniqueKey]) {
    return res.status(400).json({ success: false, message: 'No session found for this key' });
  }

  try {
    // Stop messaging
    if (stopFlags[uniqueKey]?.interval) {
      stopFlags[uniqueKey].stopped = true;
      clearInterval(stopFlags[uniqueKey].interval);
    }
    delete stopFlags[uniqueKey];
    delete messageQueues[uniqueKey];

    // Logout and close socket
    if (activeSockets[uniqueKey]) {
      try {
        await activeSockets[uniqueKey].logout();
        delete activeSockets[uniqueKey];
      } catch (logoutError) {
        console.log(chalk.yellow(`Logout failed, force closing: ${logoutError.message}`));
        delete activeSockets[uniqueKey];
      }
    }

    // Clean up session
    if (userSessions[uniqueKey]) {
      const sessionPath = `./session/${uniqueKey}`;
      if (fs.existsSync(sessionPath)) {
        try {
          fs.rmdirSync(sessionPath, { recursive: true });
          console.log(chalk.green(`Session folder deleted for ${uniqueKey}`));
        } catch (e) {
          console.log(chalk.yellow(`Could not delete session folder: ${e.message}`));
        }
      }
      delete userSessions[uniqueKey];
      saveSessions();
    }

    console.log(chalk.red(`✅ Process completely stopped for key ${uniqueKey}`));
    res.json({ success: true, message: `Process stopped successfully for key: ${uniqueKey}` });
  } catch (error) {
    console.error(chalk.red(`Error stopping process for key ${uniqueKey}: ${error.message}`));
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
  console.log(chalk.green(`✅ Server running on port ${PORT}`));
  console.log(chalk.cyan(`🌐 CORS enabled for all origins`));
  console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));
  
  await restoreSessions();
});