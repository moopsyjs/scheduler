import type { MoopsyServer } from '@moopsyjs/server';
import type { Db, Collection, WithId } from 'mongodb';

const DEFAULT_CHECK_INTERVAL = 10000;
const DEFAULT_RECAPTURE_DELAY = 120_000;
const DEFAULT_EXPIRY_DELAY = 86400000;

export interface ScheduledEventDBEntry {
  name: string
  params: any
  on: Date
  assignedServer: string | null
  running: boolean
  repeat: number | null
  uniqueKey: string | null
  captured?: Date;
}

export interface SchedulerConfig {
  /**
   * How often to check for events to run.
   * 
   * Default 10000 (10 seconds)
   */
  checkInterval: number;
  /**
   * How long before events captured by one server must go non-completed before another server claims it.
   * 
   * This behavior is to prevent a server from capturing an event and then going offline, causing the event to never run.
   * 
   * If this value is less than the time it takes to run your longest event, other server nodes may capture the event
   * even though it is still running on the original server.
   * 
   * Default 120000 (2 minutes)
   */
  recaptureDelay: number;
  /**
   * How long before events, regardless of status, are cleared from the database.
   * 
   * Default 86400000 (24 hours)
   */
  expiryDelay: number;

  /**
   * Enable verbose logs
   * 
   * Default false
   */
  verbose?: boolean;
}

export class Scheduler {
  server: MoopsyServer<any, any>;
  db: Db;
  collection: Collection<ScheduledEventDBEntry>;
  handlers: {[k: string]: (...params: any) => Promise<any>} = {};
  private readonly checkInterval: number;
  private readonly recaptureDelay: number;
  private readonly expiryDelay: number;

  public constructor (server: MoopsyServer<any, any>, db: Db, private config?: Partial<SchedulerConfig>) {
    this.server = server;
    this.db = db;
    this.collection = db.collection<ScheduledEventDBEntry>('_scheduled-events');
    this.checkInterval = config?.checkInterval ?? DEFAULT_CHECK_INTERVAL;
    this.recaptureDelay = config?.recaptureDelay ?? DEFAULT_RECAPTURE_DELAY;
    this.expiryDelay = config?.expiryDelay ?? DEFAULT_EXPIRY_DELAY;

    this.collection.createIndex({ assignedServer: 1 });

    void this.startup();

    process.on('exit', () => {
      void this.collection.updateMany({ assignedServer: this.server.serverId }, { $set: { assignedServer: null } });
    });
  }

  public readonly register = (name: string, fn: (...params: any) => Promise<any>): void => {
    this.handlers[name] = fn;
  };

  private readonly startup = async (): Promise<void> => {
    await this.collection.deleteMany({
      on: { $lt: new Date(Date.now() - this.expiryDelay) }
    });

    await this.collection.updateMany({
      $or: [
        {captured: {$exists:false}},
        {captured: {$lt: new Date(Date.now() - this.recaptureDelay)}}
      ]
    }, { $set: { assignedServer: this.server.serverId } });

    // Claim all events that have no assigned server every 60s
    setInterval(async () => {
      await this.collection.updateMany({ assignedServer: null }, { $set: { assignedServer: this.server.serverId, captured: new Date() } });
    }, 60 * 1000)

    // Check for events and run every 10s
    setInterval(async () => {
      const eventsToRun = await this.collection.find({
        $and: [
          { assignedServer: this.server.serverId },
          { on: { $lt: new Date(Date.now() + this.checkInterval) } },
          { running: false }
        ]
      }).toArray();

      // Set all fetched events to running so they don't get picked up by another server or re-run by this one
      await this.collection.updateMany({ _id: { $in: eventsToRun.map(i => i._id) } }, { $set: { running: true } });

      for (const event of eventsToRun) {
        if(this.config?.verbose === true) console.log(`[@moopsyjs/scheduler] Running event ${event.name}`);
        void this._run(event);
      }
    }, this.checkInterval);
  };

  private readonly _run = async (event: WithId<ScheduledEventDBEntry>): Promise<void> => {
    try {
      if (!(event.name in this.handlers)) {
        throw new Error(`Handler "${event.name}" was never registered`);
      }

      await this.handlers[event.name](event.params);

      await this.collection.deleteOne({ _id: event._id });
    } catch (e) {
      console.error('Failed to run', event.name, e);
    } finally {
      if (event.repeat == null) { // Event does not repeat
        await this.collection.updateOne({ _id: event._id }, { $set: { running: false } });
      } else { // Event does repeat
        await this.collection.deleteOne({ _id: event._id });
        // @ts-expect-error doesn't like us deleting _id
        delete event._id; // We just deleted the previous event, but just to be safe in case of race conditions on the DB level we remove the _id field
        await this.collection.insertOne({
          ...event,
          assignedServer: this.server.serverId,
          on: new Date(event.on.valueOf() + event.repeat)
        });
      }
    }    
  }

  public readonly scheduleEvent = async (name: string, params: any, on: Date, repeat: number | null, uniqueKey: string | null): Promise<void> => {
    if (uniqueKey !== null) {
      await this.collection.deleteMany({ uniqueKey });
    }

    await this.collection.insertOne({
      name,
      params,
      on,
      assignedServer: this.server.serverId,
      running: false,
      repeat,
      uniqueKey,
      captured: new Date()
    });
  };
}