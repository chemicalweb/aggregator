#!/usr/bin/env python
import feedparser
import csv
import threading
import Queue
import pdb

class Feed:
    """
    A RSS/Atom feed.

    Functions:
        fetch        Get the lastest feed entries and return list of entry ids
    Attributes:
        info         A dictionary of info about the Feed the user may provide
        entries      A dictionary of entries, keyed on id/guid
        ids          List of ids in the order observed from the Feed
        url          The web address of the Feed
        title        The title of the Feed
        author       The author of the Feed
    """

    def __init__(self, url, info=None):
        """
        Build a new Feed

        Parameters:
            url               The web address to the feed
            info (optional)   Optional information to store about the Feed
        """

        # set the extra info about the feed
        self.info = info
        # dictionary of all entries, keyed by id/guid
        self.entries = {}
        # list of ids in the order observed from the feed
        self.ids = []
        # web address of the feed
        self.url = url
        # tile of the feed
        self.title = ''
        # author of the feed
        self.author = ''
        
        # etag and modified are optimizations that might be supported by the
        # server providing the feed.  Feeds may provide an etag or modified
        # timestamp which you may provide the server in the header of your
        # next get request.  If the server supports either of these protocols
        # it will save bandwidth by preventing the server from sending anything
        # you have already seen.
        self._etag = None
        self._modified = None

    def get(self):
        """
        Get the contents of the feed from its url.
        """

        d = feedparser.parse(self.url, modified=self._modified,  etag=self._etag)
        # update the etag and/or modified variables, if they are given
        if 'etag' in d:
            self._etag = d.etag
        if 'modified' in d:
            self._modified = d.modified
        if 'title' in d.feed:
            self.title = d.feed['title']
        if 'author' in d.feed:
            self.author = d.feed['author']

        return d

    def fetch(self, update=False):
        """
        Get the latest entries from the feed. The entries and ids attributes
        will be updated with the lastest entries.

        Parameters:
            update (optional)    Even if an entry has already been seen, update 
                                 the entries dictionary.

        Returns:
            The number of new entries handled.
        """

        # get the feed
        d = self.get()
        # The ids of new entries found
        ids = []

        for entry in d.entries:
            if 'id' in entry and (update or (not entry.id in self.entries)):
                # update the entries dictionary
                self.entries[entry.id] = entry
                # append the id to the Feed's id list
                self.ids.append(entry.id)
                # append the id to the list of newly found entries
                ids.append(entry.id)

        # return the list of new entries
        return ids

    def __getitem__(self, key):
        return self.entries[key]

    def __setitem__(self, key, value):
        self.entries[key] = value

    def __delitem__(self, key):
        del self.entries[key]

    def __len__(self):
        return len(self.entries)
    
    # End of class Feed


class ThreadFeed(threading.Thread):
    """
    Grabs the latest contents of a feed using Feed objects.
    """

    def __init__(self, queue, idQueue):
        """
        Create a new ThreadFeed.

        Parameters:
            queue     The Queue.Queue which has the Feed objects to check
            idQueue   The Queue.Queue which entries found will be added to 
                      for processing by ThreadProcessEntry
        """

        threading.Thread.__init__(self)
        self.queue = queue
        self.idQueue = idQueue

    def run(self):
        while True:
            feed = self.queue.get()
            ids = feed.fetch()

            for id in ids:
                # add the entry that was found to the queue for the
                # ThreadProcessEntry threads
                self.idQueue.put((feed, id))

            self.queue.task_done()

class ThreadProcessEntry(threading.Thread):
    """
    Process an entry found in a feed.  If a callback is provided to the 
    constructor, it will be called for each new entry.  The callback should
    take 2 parameters, 1) the Feed instance from which the entry is from, and
    2) the id of the new entry.
    """

    def __init__(self, idQueue, callback=None):
        """
        Create a new ThreadProcessEntry which processes entries found on a Feed.

        Parameters:
            idQueue     The Queue.Queue where entry ids are stored
            callback    The callback function (feed, id) that should be called
                        when a new entry is found
        """
        threading.Thread.__init__(self)
        self.idQueue = idQueue
        self.callback = callback

    def run(self):
        while True:
            feed, id = self.idQueue.get()

            if not self.callback is None:
                self.callback(feed, id)

            self.idQueue.task_done()

class FeedList:
    """
    A list of RSS/Atom Feeds.
    
    Functions:
        fetch     Get the latest entries from each Feed in the list
        loadFile  Load a file listing feeds into the FeedList
        addFeed   Add a feed to the list given an url and optional extra info
        poolSize  Number of threads that the FeedList currently is using
        threaded  Check if the FeedList is running threaded
        addFeed   Add a Feed instance to the FeedList
        addFeedThread          Add a thread to the pool for downloading feeds
        addProcessEntryThread  Add a thread to the pool for processing entries
    """

    def __init__(self, file=None, threaded=False, poolSize=5):
        """
        Create a new FeedList.

        Parameters:
            file      (optional) Filename to load feed urls and info from
        """

        # public attributes
        self.feeds = []

        # private attributes
        self._initialPoolSize = poolSize
        self._poolSize = 0
        self._threaded = threaded
        self._feedQueue = Queue.Queue()
        self._idQueue = Queue.Queue()
        self._threadsCreated = False

        if not file is None:
            self.loadFile(file)

    def addFeed(self, url, info=None):
        """
        Add a Feed instance to the list, given an url and optional extra info.

        Parameters:
            url     the url of the feed
            info    (optional) the extra information you want to track

        Example:
            feeds.addFeed('http://www.abc.com/rss.xml')
            feeds.addFeed('http://www.xyz.com/rss.xml', info='feed for xyz')
        """

        self.feeds.append(Feed(url, info=info))

    def loadFile(self, file, headers=None):
        """
        Load a list of urls from a file.

        Parameters:
            file     The file path to load
            headers  (optional) list of headers, if there is more than one row
                     in the list, the LAST one will be used as the feed's URL
                     regardless of what it's column header is.
        """

        with open(file, 'rb') as handle:
            # read a small sample of the file to see how it is composed
            sample = handle.read(1024)
            handle.seek(0)
            dialect = csv.Sniffer().sniff(sample)
            # construct reader after the delimeter and layout have been detected
            reader = csv.reader(handle, dialect)

            # if list of headers is provided, don't add the first line as a feed
            if headers is None:
                startLine = 0
            else:
                startLine = 1

            # read the lines of the file
            for line in reader:
                if reader.line_num > startLine:
                    # TODO: Check for duplicates before adding
                    self.addFeed(line[-1], info=dict(zip(headers[:-1], line[:-1])))

    def addFeedThread(self):
        """
        Add a thread that can download feeds
        """

        thread = ThreadFeed(self._feedQueue, self._idQueue)
        thread.setDaemon(True)
        thread.start()
        self._poolSize += 1

    def addProcessEntryThread(self, callback=None):
        """
        Add a thread that can process downloaded feed entries.

        Parameters:
            callback     (optional) function to be called (feed, id) when a 
                         new entry is found
        """

        thread = ThreadProcessEntry(self._idQueue, callback=callback)
        thread.setDaemon(True)
        thread.start()

    def fetchThreaded(self, callback=None):
        """
        Fetch the latest updates from each Feed using threads.
        
        Parameters:
            poolSize     (optional) The number of threads
            callback     (optional) The func (feed, id) to call when entry found
        """
        
        if not self._threadsCreated:
            # if the threads have not yet been created, create them
            for i in range(self._initialPoolSize):
                self.addFeedThread()

            for i in range(self._initialPoolSize):
                self.addProcessEntryThread(callback=callback)

            # Note that the threads have been created so that calling fetch
            # multiple times won't keep creating threads
            self._threadsCreated = True

        for feed in self.feeds:
            self._feedQueue.put(feed)

        # wait until the threads are completed to proceed
        self.wait()

    def wait(self):
        """
        Wait until threads have finished working
        """
        self._feedQueue.join()
        self._idQueue.join()

    def fetchSerially(self, callback=None):
        """
        Fetch the lastest entries for each feed.
        
        Parameters:
            callback     A function which will be called once for each new entry
        """
        for feed in self.feeds:
            ids = feed.fetch()

            # invoke the callback if provided
            if not callback is None:
                for id in ids:
                    callback(feed, id)

    def fetch(self, callback=None):
        """
        Fetch the latest entries from the Feeds in this list.  If the 'threaded'
        property is set to True, it will perform a multi-threaded fetch using
        the 'poolSize' property as the number of threads to run simultaneously.

        Parameters:
            callback     (optional) A function which will be called for each 
                         new entry. It should take 2 parameters, feed, and id.
                         The feed parameter is the Feed instance to which the
                         entry identified by 'id' belongs
        """

        if self.threaded:
            self.fetchThreaded(callback=callback)
        else:
            self.fetchSerially(callback=callback)


    def threaded(self):
        """
        Return True if the feed is threaded
        """
        
        return self._threaded

    def poolSize(self):
        """
        Get the number of threads in this FeedList's thread pool
        """

        return self._poolSize

    def __iter__(self):
        for feed in self.feeds:
            yield feed

    def __getitem__(self, key):
        return self.feeds[key]

    def __setitem__(self, key, value):
        self.feeds[key] = value

    def __delitem__(self, key):
        del self.feeds[key]

    def __len__(self):
        return len(self.feeds)

    # End class FeedList

def entryFound(feed, id):
    entryTitle = feed[id].title
    feedTitle = feed.title
    print "%s --- %s" % (feedTitle, entryTitle)

if __name__ == '__main__':
    import time

    feedsThreaded = FeedList(file="feeds", threaded=True)
    feedsSerial = FeedList(file="feeds", threaded=False)

    threadStart = time.time()
    feedsThreaded.fetch(callback=entryFound)
    threadEnd = time.time()

    serialStart = time.time()
    feedsSerial.fetch(callback=entryFound)
    serialEnd = time.time()

    print ("Threaded: %f, Serial: %s" % (threadEnd - threadStart, 
                                        serialEnd - serialStart))
