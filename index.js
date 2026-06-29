import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import multer from 'multer';
import makeWASocket, {
  useMultiFileAuthState,
  makeCacheableSignalKeyStore,
  DisconnectReason,
  Browsers,
  fetchLatestBaileysVersion
} from '@whiskeysockets/baileys';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { Boom } from '@hapi/boom';
import cors from 'cors';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Ensure required directories exist ──────────────────────────────────────────
['./session', './uploads', './public'].forEach(dir => {
  const full = path.resolve(__dirname, dir);
  if (!fs.existsSync(full)) fs.mkdirSync(full, { recursive: true });
});

// ── Multer: store files in 'uploads' ────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, path.resolve(__dirname, 'uploads')),
  filename: (req, file, cb) => {
    const unique = crypto.randomBytes(8).toString('hex');
    const ext = path.extname(file.originalname);
    cb(null, `${Date.now()}-${unique}${ext}`);
  }
});
const upload = multer({ storage, limits: { fileSize: 50 * 1024 * 1024 } });

app.use(cors({ origin: '*', credentials: true }));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static('public'));

// ── In-memory state ───────────────────────────────────────────────────────────
const SESSION_FILE      = './running_sessions.json';
const userSessions      = {};
const stopFlags         = {};
const activeSockets     = {};
const messageQueues     = {};
const reconnectAttempts = {};
const sessionStats      = {};
const reconnectTimeouts = {}; // Store timeout IDs

// ── Helpers ───────────────────────────────────────────────────────────────────
const saveSessions = () => {
  try {
    fs.writeFileSync(SESSION_FILE, JSON.stringify(userSessions, null, 2), 'utf8');
  } catch (e) {
    console.error(chalk.red(`Save sessions error: ${e.message}`));
  }
};

const removeDir = (dirPath) => {
  try {
    if (fs.existsSync(dirPath)) fs.rmSync(dirPath, { recursive: true, force: true });
  } catch (e) {}
};

const generateUniqueKey = () => crypto.randomBytes(16).toString('hex');

const cleanupSession = (uniqueKey) => {
  // Clear any pending timeouts
  if (reconnectTimeouts[uniqueKey]) {
    clearTimeout(reconnectTimeouts[uniqueKey]);
    delete reconnectTimeouts[uniqueKey];
  }
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
    delete stopFlags[uniqueKey].interval;
  }
  delete stopFlags[uniqueKey];
  delete messageQueues[uniqueKey];
  delete activeSockets[uniqueKey];
};

// ── Core: Message Sender (Continuous Loop) ──────────────────────────────────
const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  // Clear any existing intervals
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = { 
      messages: [...messages], 
      currentIndex: 0, 
      isSending: false 
    };
  }

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type: 'msg' };
  }

  const queue = messageQueues[uniqueKey];

  const sendNext = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      return;
    }
    
    // If socket is not connected, try to reconnect
    if (!activeSockets[uniqueKey]) {
      console.log(chalk.yellow(`⚠️ Socket not active for ${uniqueKey}, waiting for reconnect...`));
      return;
    }
    
    if (queue.isSending || queue.messages.length === 0) {
      return;
    }

    queue.isSending = true;

    let chatId;
    if (target.includes('@g.us') || target.includes('@s.whatsapp.net')) {
      chatId = target;
    } else {
      chatId = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    }

    const msg       = queue.messages[queue.currentIndex];
    const formatted = hatersName ? `${hatersName} ${msg}` : msg;

    try {
      await MznKing.sendMessage(chatId, { text: formatted });
      sessionStats[uniqueKey].sent++;
      sessionStats[uniqueKey].lastMessage = formatted.substring(0, 60);
      console.log(chalk.green(`✉️ [${sessionStats[uniqueKey].sent}] → ${chatId}`));

      queue.currentIndex++;
      if (queue.currentIndex >= queue.messages.length) {
        console.log(chalk.cyan(`🔄 All messages done — restarting loop (infinite)`));
        queue.currentIndex = 0;
      }
    } catch (err) {
      sessionStats[uniqueKey].failed++;
      console.error(chalk.red(`❌ Send failed: ${err.message}`));
      
      // If error is connection related, mark socket as inactive
      if (err.message.includes('connection') || err.message.includes('socket')) {
        console.log(chalk.yellow(`⚠️ Connection issue detected, will retry...`));
        // Don't stop, just wait for next interval
      }
    } finally {
      queue.isSending = false;
    }
  };

  // Use setInterval with proper cleanup
  const intervalId = setInterval(() => {
    // If stopped, clear interval
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(intervalId);
      return;
    }
    sendNext();
  }, parseInt(speed) * 1000);

  // Store interval ID for cleanup
  if (!stopFlags[uniqueKey]) {
    stopFlags[uniqueKey] = { stopped: false };
  }
  stopFlags[uniqueKey].interval = intervalId;
  
  console.log(chalk.cyan(`📨 Messaging started continuously every ${speed}s → ${target}`));
  
  // Send first message immediately
  setTimeout(sendNext, 1000);
};

// ── Core: Photo Sender (Continuous Loop) ────────────────────────────────────
const startPhotoSending = (MznKing, uniqueKey, target, caption, photoItems, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = { items: [...photoItems], currentIndex: 0, isSending: false };
  }

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type: 'photo' };
  }

  const queue = messageQueues[uniqueKey];

  const sendNext = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      return;
    }
    if (!activeSockets[uniqueKey]) {
      console.log(chalk.yellow(`⚠️ Socket not active for ${uniqueKey}, waiting...`));
      return;
    }
    if (queue.isSending || queue.items.length === 0) return;

    queue.isSending = true;

    let chatId;
    if (target.includes('@g.us') || target.includes('@s.whatsapp.net')) {
      chatId = target;
    } else {
      chatId = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    }

    const item = queue.items[queue.currentIndex];

    try {
      let imagePayload;
      if (item.startsWith('http://') || item.startsWith('https://')) {
        imagePayload = { url: item };
      } else {
        const filePath = path.resolve(__dirname, item);
        if (!fs.existsSync(filePath)) {
          console.log(chalk.yellow(`⚠️ Photo not found: ${filePath}, skipping...`));
          queue.currentIndex++;
          if (queue.currentIndex >= queue.items.length) queue.currentIndex = 0;
          queue.isSending = false;
          return;
        }
        imagePayload = fs.readFileSync(filePath);
      }

      await MznKing.sendMessage(chatId, {
        image: imagePayload,
        caption: caption || ''
      });

      sessionStats[uniqueKey].sent++;
      sessionStats[uniqueKey].lastMessage = `Photo ${queue.currentIndex + 1}`;
      console.log(chalk.green(`📸 [${sessionStats[uniqueKey].sent}] Photo sent → ${chatId}`));

      queue.currentIndex++;
      if (queue.currentIndex >= queue.items.length) {
        console.log(chalk.cyan(`🔄 All photos done — restarting loop (infinite)`));
        queue.currentIndex = 0;
      }
    } catch (err) {
      sessionStats[uniqueKey].failed++;
      console.error(chalk.red(`❌ Photo send failed: ${err.message}`));
      queue.currentIndex++;
      if (queue.currentIndex >= queue.items.length) queue.currentIndex = 0;
    } finally {
      queue.isSending = false;
    }
  };

  const intervalId = setInterval(() => {
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(intervalId);
      return;
    }
    sendNext();
  }, parseInt(speed) * 1000);

  if (!stopFlags[uniqueKey]) {
    stopFlags[uniqueKey] = { stopped: false };
  }
  stopFlags[uniqueKey].interval = intervalId;
  
  console.log(chalk.cyan(`📸 Photo sending started continuously every ${speed}s → ${target}`));
  setTimeout(sendNext, 1000);
};

// ── Core: Sticker Sender (Continuous Loop) ──────────────────────────────────
const startStickerSending = (MznKing, uniqueKey, target, stickerPath, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type: 'sticker' };
  }

  const sendNext = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      return;
    }
    if (!activeSockets[uniqueKey]) {
      console.log(chalk.yellow(`⚠️ Socket not active for ${uniqueKey}, waiting...`));
      return;
    }

    let chatId;
    if (target.includes('@g.us') || target.includes('@s.whatsapp.net')) {
      chatId = target;
    } else {
      chatId = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    }

    try {
      const filePath = path.resolve(__dirname, stickerPath);
      if (!fs.existsSync(filePath)) {
        console.log(chalk.yellow(`⚠️ Sticker file missing: ${filePath}, waiting...`));
        return;
      }
      const stickerBuffer = fs.readFileSync(filePath);

      await MznKing.sendMessage(chatId, { sticker: stickerBuffer });

      sessionStats[uniqueKey].sent++;
      sessionStats[uniqueKey].lastMessage = `Sticker #${sessionStats[uniqueKey].sent}`;
      console.log(chalk.green(`🎭 [${sessionStats[uniqueKey].sent}] Sticker sent → ${chatId}`));
    } catch (err) {
      sessionStats[uniqueKey].failed++;
      console.error(chalk.red(`❌ Sticker send failed: ${err.message}`));
    }
  };

  const intervalId = setInterval(() => {
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(intervalId);
      return;
    }
    sendNext();
  }, parseInt(speed) * 1000);

  if (!stopFlags[uniqueKey]) {
    stopFlags[uniqueKey] = { stopped: false };
  }
  stopFlags[uniqueKey].interval = intervalId;
  
  console.log(chalk.cyan(`🎭 Sticker sending started continuously every ${speed}s → ${target}`));
  setTimeout(sendNext, 1000);
};

// ── WhatsApp Connection with Improved Reconnection ──────────────────────────
const connectAndLogin = async (phoneNumber, uniqueKey, sendPairingCode = null) => {
  const sessionPath    = `./session/${uniqueKey}`;
  let pairingCodeSent  = false;
  let isReconnecting = false;

  const startConnection = async () => {
    // If session is stopped, don't reconnect
    if (stopFlags[uniqueKey]?.stopped) {
      console.log(chalk.yellow(`⚠️ Session ${uniqueKey} stopped, not reconnecting`));
      return;
    }

    try {
      console.log(chalk.magenta(`🚀 Connecting ${phoneNumber} [${uniqueKey}]`));

      if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version }          = await fetchLatestBaileysVersion();

      const MznKing = makeWASocket({
        version,
        logger: pino.default({ level: 'silent' }),
        browser: Browsers.windows('Firefox'),
        auth: {
          creds: state.creds,
          keys:  makeCacheableSignalKeyStore(state.keys, pino.default({ level: 'silent' }))
        },
        printQRInTerminal:            false,
        generateHighQualityLinkPreview: true,
        markOnlineOnConnect:          true,
        getMessage:                   async () => undefined,
        keepAliveIntervalMs:          30000,
        connectTimeoutMs:             60000,
        defaultQueryTimeoutMs:        undefined,
        retryRequestDelayMs:          250,
      });

      activeSockets[uniqueKey] = MznKing;

      if (!MznKing.authState.creds.registered && !pairingCodeSent && sendPairingCode) {
        await new Promise(r => setTimeout(r, 2000));
        try {
          const cleaned = phoneNumber.replace(/[^0-9]/g, '');
          console.log(chalk.cyan(`🔐 Requesting pairing code for ${cleaned}...`));
          const code        = await MznKing.requestPairingCode(cleaned);
          const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
          console.log(chalk.green(`✅ Pairing Code: ${pairingCode}`));
          pairingCodeSent = true;
          sendPairingCode(pairingCode, false);
        } catch (err) {
          console.error(chalk.red(`❌ Pairing error: ${err.message}`));
          if (!pairingCodeSent && sendPairingCode) {
            pairingCodeSent = true;
            sendPairingCode(null, false, err.message);
          }
          // Don't stop, retry later
          setTimeout(() => startConnection(), 5000);
          return;
        }
      } else if (MznKing.authState.creds.registered) {
        console.log(chalk.green(`✅ Session already registered for ${uniqueKey}`));
        if (!pairingCodeSent && sendPairingCode) {
          pairingCodeSent = true;
          sendPairingCode(null, true);
        }
      }

      // Set up event handlers
      MznKing.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
          console.log(chalk.green(`✅✅✅ Connected! [${uniqueKey}]`));
          reconnectAttempts[uniqueKey] = 0;
          isReconnecting = false;

          // Clear any pending reconnect timeout
          if (reconnectTimeouts[uniqueKey]) {
            clearTimeout(reconnectTimeouts[uniqueKey]);
            delete reconnectTimeouts[uniqueKey];
          }

          userSessions[uniqueKey] = {
            ...userSessions[uniqueKey],
            phoneNumber,
            uniqueKey,
            connected:          true,
            lastUpdateTimestamp: Date.now()
          };
          saveSessions();

          if (!pairingCodeSent && sendPairingCode) {
            pairingCodeSent = true;
            sendPairingCode(null, true);
          }

          const sess = userSessions[uniqueKey];

          // Resume all active tasks
          if (sess?.messaging && sess?.messages) {
            const { target, hatersName, messages, speed } = sess;
            console.log(chalk.cyan(`🔄 Resuming message sending for ${uniqueKey}...`));
            if (!messageQueues[uniqueKey]) {
              messageQueues[uniqueKey] = { messages: [...messages], currentIndex: 0, isSending: false };
            }
            startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
          }

          if (sess?.photoing && sess?.photoItems) {
            const { target, caption, photoItems, speed } = sess;
            console.log(chalk.cyan(`🔄 Resuming photo sending for ${uniqueKey}...`));
            if (!messageQueues[uniqueKey]) {
              messageQueues[uniqueKey] = { items: [...photoItems], currentIndex: 0, isSending: false };
            }
            startPhotoSending(MznKing, uniqueKey, target, caption, photoItems, speed);
          }

          if (sess?.stickering && sess?.stickerPath) {
            const { target, stickerPath, speed } = sess;
            console.log(chalk.cyan(`🔄 Resuming sticker sending for ${uniqueKey}...`));
            startStickerSending(MznKing, uniqueKey, target, stickerPath, speed);
          }
        }

        if (connection === 'close') {
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
          console.log(chalk.red(`⚠️ Disconnected — reason: ${reason}`));

          // Don't reconnect if stopped
          if (stopFlags[uniqueKey]?.stopped) {
            console.log(chalk.yellow(`⏹️ Session stopped, not reconnecting`));
            return;
          }

          if (reason === DisconnectReason.badSession) {
            console.log(chalk.red(`Bad session — deleting & reconnecting`));
            removeDir(sessionPath);
          } else if (reason === DisconnectReason.connectionReplaced) {
            console.log(chalk.red(`Connection replaced — will reconnect`));
            // Cleanup and reconnect
            cleanupSession(uniqueKey);
            // Try to reconnect after delay
            scheduleReconnect(uniqueKey, phoneNumber);
            return;
          } else if (reason === DisconnectReason.loggedOut || reason === 401) {
            console.log(chalk.red(`Logged out — cleaning up`));
            removeDir(sessionPath);
            cleanupSession(uniqueKey);
            if (userSessions[uniqueKey]) {
              userSessions[uniqueKey].connected = false;
              userSessions[uniqueKey].messaging = false;
              userSessions[uniqueKey].photoing  = false;
              userSessions[uniqueKey].stickering = false;
              saveSessions();
            }
            // Don't automatically reconnect on logout
            return;
          }

          // Always try to reconnect for other errors
          if (!isReconnecting) {
            scheduleReconnect(uniqueKey, phoneNumber);
          }
        }
      });

      MznKing.ev.on('creds.update', saveCreds);
      MznKing.ev.on('messages.upsert', () => {});

    } catch (error) {
      console.error(chalk.red(`❌ Connection error: ${error.message}`));
      if (!pairingCodeSent && sendPairingCode) {
        pairingCodeSent = true;
        sendPairingCode(null, false, error.message);
      }
      if (!stopFlags[uniqueKey]?.stopped) {
        scheduleReconnect(uniqueKey, phoneNumber);
      }
    }
  };

  // Schedule reconnection with exponential backoff
  const scheduleReconnect = (key, phone) => {
    if (stopFlags[key]?.stopped) {
      console.log(chalk.yellow(`⏹️ Session ${key} stopped, not scheduling reconnect`));
      return;
    }

    // Clear existing timeout
    if (reconnectTimeouts[key]) {
      clearTimeout(reconnectTimeouts[key]);
      delete reconnectTimeouts[key];
    }

    reconnectAttempts[key] = (reconnectAttempts[key] || 0) + 1;
    // Exponential backoff with max 60 seconds
    const delay = Math.min(5000 * Math.pow(1.5, reconnectAttempts[key]), 60000);
    
    console.log(chalk.yellow(`🔄 Scheduling reconnect in ${Math.round(delay/1000)}s (attempt ${reconnectAttempts[key]}) for ${key}`));
    
    reconnectTimeouts[key] = setTimeout(async () => {
      if (stopFlags[key]?.stopped) {
        console.log(chalk.yellow(`⏹️ Session ${key} stopped during reconnect wait`));
        return;
      }
      console.log(chalk.cyan(`🔄 Attempting reconnect for ${key}...`));
      await startConnection();
    }, delay);
  };

  await startConnection();
};

// ── Session restore on startup ────────────────────────────────────────────────
const restoreSessions = async () => {
  if (!fs.existsSync(SESSION_FILE)) return;

  try {
    const data          = fs.readFileSync(SESSION_FILE, 'utf8');
    const savedSessions = JSON.parse(data);
    Object.assign(userSessions, savedSessions);

    console.log(chalk.green(`📂 Found ${Object.keys(userSessions).length} saved sessions`));

    for (const [, session] of Object.entries(userSessions)) {
      if (!session.phoneNumber || !session.uniqueKey) continue;
      const sessionPath = `./session/${session.uniqueKey}`;
      if (!fs.existsSync(sessionPath)) continue;

      console.log(chalk.cyan(`🔄 Restoring: ${session.uniqueKey} (${session.phoneNumber})`));
      stopFlags[session.uniqueKey]         = { stopped: false };
      reconnectAttempts[session.uniqueKey] = 0;

      if (session.messaging && session.messages) {
        messageQueues[session.uniqueKey] = { messages: [...session.messages], currentIndex: 0, isSending: false };
      }
      if (session.photoing && session.photoItems) {
        messageQueues[session.uniqueKey] = { items: [...session.photoItems], currentIndex: 0, isSending: false };
      }

      await connectAndLogin(session.phoneNumber, session.uniqueKey, null);
      await new Promise(r => setTimeout(r, 1000));
    }

    console.log(chalk.green(`✅ Session restoration complete!`));
  } catch (err) {
    console.error(chalk.red(`Error loading sessions: ${err.message}`));
  }
};

// ═══════════════════════════════════════════════════════════════════════════════
//  ROUTES (Unchanged - same as before)
// ═══════════════════════════════════════════════════════════════════════════════

// ── GET /ping ── Health check ──────────────────────────────────────────────
app.get('/ping', (req, res) => {
  res.json({ success: true, message: 'Server is alive!', timestamp: Date.now() });
});

// ── POST /login ───────────────────────────────────────────────────────────────
app.post('/login', async (req, res) => {
  try {
    let { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone number is required!' });

    phoneNumber = phoneNumber.replace(/[^0-9]/g, '');
    console.log(chalk.cyan(`📞 Login: ${phoneNumber}`));

    const uniqueKey = generateUniqueKey();
    stopFlags[uniqueKey]         = { stopped: false };
    reconnectAttempts[uniqueKey] = 0;

    const sendPairingCode = (pairingCode, isConnected = false, errorMsg = null) => {
      if (errorMsg) {
        res.json({ success: false, message: 'Error generating pairing code', error: errorMsg, uniqueKey });
      } else if (isConnected) {
        res.json({ success: true, message: 'WhatsApp Connected!', connected: true, uniqueKey });
      } else {
        res.json({ success: true, message: 'Pairing code generated', pairingCode, uniqueKey });
      }
    };

    await connectAndLogin(phoneNumber, uniqueKey, sendPairingCode);
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ── POST /getGroupUID ─────────────────────────────────────────────────────────
app.post('/getGroupUID', async (req, res) => {
  try {
    const { uniqueKey } = req.body;
    if (!uniqueKey)               return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No active session' });
    if (!activeSockets[uniqueKey]) return res.status(400).json({ success: false, message: 'WhatsApp not connected' });

    const MznKing = activeSockets[uniqueKey];
    await new Promise(r => setTimeout(r, 1000));

    const groups    = await MznKing.groupFetchAllParticipating();
    const groupUIDs = Object.values(groups).map(g => ({ groupName: g.subject, groupId: g.id }));

    console.log(chalk.green(`✅ Fetched ${groupUIDs.length} groups for ${uniqueKey}`));
    res.json({ success: true, groupUIDs });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error fetching groups' });
  }
});

// ── POST /startMessaging ──────────────────────────────────────────────────────
app.post('/startMessaging', upload.single('messageFile'), async (req, res) => {
  try {
    const { uniqueKey, target, hatersName, speed } = req.body;
    const filePath = req.file?.path;

    if (!uniqueKey || !target || !speed)
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    if (!userSessions[uniqueKey])
      return res.status(400).json({ success: false, message: 'Invalid session key!' });
    if (!activeSockets[uniqueKey])
      return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });
    if (!filePath)
      return res.status(400).json({ success: false, message: 'No message file uploaded!' });

    let messages = [];
    try {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      messages = fileContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      if (messages.length === 0)
        return res.status(400).json({ success: false, message: 'File has no valid messages!' });
    } catch (err) {
      return res.status(500).json({ success: false, message: 'Error reading file!' });
    } finally {
      try { fs.unlinkSync(filePath); } catch (e) {}
    }

    const MznKing = activeSockets[uniqueKey];

    if (stopFlags[uniqueKey]?.interval) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete stopFlags[uniqueKey].interval;
    }
    stopFlags[uniqueKey] = { stopped: false };
    delete messageQueues[uniqueKey];
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type: 'msg' };

    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target, hatersName: hatersName || '', messages, speed,
      messaging: true, photoing: false, stickering: false
    };
    saveSessions();

    startMessaging(MznKing, uniqueKey, target, hatersName || '', messages, speed);

    res.json({ success: true, message: 'Message automation started! (Infinite loop)', uniqueKey, messageCount: messages.length, target });
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ── POST /startPhotoSending ───────────────────────────────────────────────────
app.post('/startPhotoSending', upload.fields([
  { name: 'photoFile',     maxCount: 1 },
  { name: 'photoListFile', maxCount: 1 }
]), async (req, res) => {
  try {
    const { uniqueKey, target, caption, speed, mode } = req.body;

    console.log(chalk.cyan(`📸 Photo request: ${uniqueKey}, mode=${mode}, target=${target}`));

    if (!uniqueKey || !target || !speed)
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    if (!userSessions[uniqueKey])
      return res.status(400).json({ success: false, message: 'Invalid session key!' });
    if (!activeSockets[uniqueKey])
      return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });

    let photoItems = [];

    if (mode === 'single') {
      const photoFile = req.files?.photoFile?.[0];
      if (!photoFile)
        return res.status(400).json({ success: false, message: 'No photo file uploaded!' });

      const destPath = path.resolve(__dirname, `uploads/photo_${uniqueKey}${path.extname(photoFile.originalname)}`);
      fs.renameSync(photoFile.path, destPath);
      photoItems = [destPath];
      console.log(chalk.green(`📸 Single photo saved: ${destPath}`));

    } else {
      const listFile = req.files?.photoListFile?.[0];
      if (!listFile)
        return res.status(400).json({ success: false, message: 'No photo list file uploaded!' });

      try {
        const content = fs.readFileSync(listFile.path, 'utf-8');
        photoItems = content.split('\n').map(l => l.trim()).filter(l => l.length > 0);
        if (photoItems.length === 0)
          return res.status(400).json({ success: false, message: 'Photo list file is empty!' });
        console.log(chalk.green(`📸 Loaded ${photoItems.length} photo entries from list`));
      } finally {
        try { fs.unlinkSync(listFile.path); } catch (e) {}
      }
    }

    const MznKing = activeSockets[uniqueKey];

    if (stopFlags[uniqueKey]?.interval) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete stopFlags[uniqueKey].interval;
    }
    stopFlags[uniqueKey] = { stopped: false };
    delete messageQueues[uniqueKey];
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type: 'photo' };

    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target, caption: caption || '', photoItems, speed,
      messaging: false, photoing: true, stickering: false
    };
    saveSessions();

    startPhotoSending(MznKing, uniqueKey, target, caption || '', photoItems, speed);

    res.json({ success: true, message: 'Photo sending started! (Infinite loop)', uniqueKey, photoCount: photoItems.length, target });
  } catch (error) {
    console.error(chalk.red(`❌ Photo route error: ${error.message}`));
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ── POST /startStickerSending ─────────────────────────────────────────────────
app.post('/startStickerSending', upload.single('stickerFile'), async (req, res) => {
  try {
    const { uniqueKey, target, speed } = req.body;

    console.log(chalk.cyan(`🎭 Sticker request: ${uniqueKey}, target=${target}`));

    if (!uniqueKey || !target || !speed)
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    if (!userSessions[uniqueKey])
      return res.status(400).json({ success: false, message: 'Invalid session key!' });
    if (!activeSockets[uniqueKey])
      return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });

    const stickerFile = req.file;
    if (!stickerFile)
      return res.status(400).json({ success: false, message: 'No sticker file uploaded!' });

    const destPath = path.resolve(__dirname, `uploads/sticker_${uniqueKey}${path.extname(stickerFile.originalname || '.webp')}`);
    fs.renameSync(stickerFile.path, destPath);
    console.log(chalk.green(`🎭 Sticker saved: ${destPath}`));

    const MznKing = activeSockets[uniqueKey];

    if (stopFlags[uniqueKey]?.interval) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete stopFlags[uniqueKey].interval;
    }
    stopFlags[uniqueKey] = { stopped: false };
    delete messageQueues[uniqueKey];
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', type: 'sticker' };

    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target, stickerPath: destPath, speed,
      messaging: false, photoing: false, stickering: true
    };
    saveSessions();

    startStickerSending(MznKing, uniqueKey, target, destPath, speed);

    res.json({ success: true, message: 'Sticker sending started! (Infinite loop)', uniqueKey, target });
  } catch (error) {
    console.error(chalk.red(`❌ Sticker route error: ${error.message}`));
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ── GET /sessionStatus/:uniqueKey ─────────────────────────────────────────────
app.get('/sessionStatus/:uniqueKey', (req, res) => {
  const { uniqueKey } = req.params;
  const session       = userSessions[uniqueKey];
  const stats         = sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '' };

  if (!session) return res.json({ exists: false });

  res.json({
    exists:       true,
    connected:    !!activeSockets[uniqueKey],
    messaging:    (session.messaging || session.photoing || session.stickering) && !stopFlags[uniqueKey]?.stopped,
    sent:         stats.sent,
    failed:       stats.failed,
    lastMessage:  stats.lastMessage,
    type:         stats.type || 'msg',
    target:       session.target,
    speed:        session.speed,
    messageCount: session.messages?.length || session.photoItems?.length || 1,
  });
});

// ── POST /stop ────────────────────────────────────────────────────────────────
app.post('/stop', async (req, res) => {
  const { uniqueKey } = req.body;
  if (!uniqueKey)               return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
  if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No session found' });

  try {
    // Clear any pending reconnect timeout
    if (reconnectTimeouts[uniqueKey]) {
      clearTimeout(reconnectTimeouts[uniqueKey]);
      delete reconnectTimeouts[uniqueKey];
    }

    if (stopFlags[uniqueKey]?.interval) {
      stopFlags[uniqueKey].stopped = true;
      clearInterval(stopFlags[uniqueKey].interval);
      delete stopFlags[uniqueKey].interval;
    }
    delete stopFlags[uniqueKey];
    delete messageQueues[uniqueKey];
    delete sessionStats[uniqueKey];

    if (activeSockets[uniqueKey]) {
      try { await activeSockets[uniqueKey].logout(); } catch (e) {}
      delete activeSockets[uniqueKey];
    }

    // Cleanup uploaded files
    ['photo', 'sticker'].forEach(type => {
      const pat = path.resolve(__dirname, `uploads/${type}_${uniqueKey}`);
      [pat + '.jpg', pat + '.png', pat + '.webp', pat + '.gif'].forEach(fp => {
        try { if (fs.existsSync(fp)) fs.unlinkSync(fp); } catch (e) {}
      });
    });

    const sessionPath = `./session/${uniqueKey}`;
    removeDir(sessionPath);
    delete userSessions[uniqueKey];
    saveSessions();

    console.log(chalk.red(`✅ Stopped & logged out: ${uniqueKey}`));
    res.json({ success: true, message: 'Process stopped and logged out!' });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

// ── GET / → serve index.html ──────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── Start server ──────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
  console.log(chalk.green(`✅ VEER Server running on port ${PORT}`));
  console.log(chalk.cyan(`🌐 CORS enabled for all origins`));
  console.log(chalk.cyan(`🔗 Test: http://localhost:${PORT}/ping`));
  console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));

  await restoreSessions();
});
