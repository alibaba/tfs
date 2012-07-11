/*!
 * $Id: thread.h 5 2010-09-29 07:44:56Z duanfei@taobao.com $ 
 *
 * Copyright 2008 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 *
 * \file thread.h
 * \brief thread control for KFS metadata server
 * \author Blake Lewis (Kosmix Corp.)
 */
#if !defined(KFS_THREAD_H)
#define KFS_THREAD_H

#include <cassert>
#include "config.h"

extern "C" {
#include <pthread.h>
}

#if !defined(UNUSED_ATTR)
#if defined(__GNUC__)
#define UNUSED_ATTR __attribute__((unused))
#else
#define UNUSED_ATTR
#endif
#endif

namespace KFS {

class MetaThread {
	pthread_mutex_t mutex;
	pthread_cond_t cv;
	pthread_t thread;
#if defined (__APPLE__)
	bool threadInited;
#else
	static const pthread_t NO_THREAD = -1u;
#endif
public:
	typedef void *(*thread_start_t)(void *);
#if defined (__APPLE__)
	MetaThread() : threadInited(false)
	{
		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&cv, NULL);
	}
#else
	MetaThread(): thread(NO_THREAD)
	{
		pthread_mutex_init(&mutex, NULL);
		pthread_cond_init(&cv, NULL);
	}
#endif
	~MetaThread()
	{
		pthread_mutex_destroy(&mutex);
#if defined (__APPLE__)
		if (threadInited) {
                	int UNUSED_ATTR status = pthread_cancel(thread);
			assert(status == 0);
		}
#else
                if (thread != NO_THREAD) {
			//int UNUSED_ATTR status = pthread_cancel(thread);
			//assert(status == 0);
                }
#endif
		pthread_cond_destroy(&cv);
	}
	void lock()
	{
		int UNUSED_ATTR status = pthread_mutex_lock(&mutex);
		assert(status == 0);
	}
	void unlock()
	{
		int UNUSED_ATTR status = pthread_mutex_unlock(&mutex);
		assert(status == 0);
       	}
	void wakeup()
	{
		int UNUSED_ATTR status = pthread_cond_broadcast(&cv);
		assert(status == 0);
	}
	void sleep()
	{
		int UNUSED_ATTR status = pthread_cond_wait(&cv, &mutex);
		assert(status == 0);
	}
	void start(thread_start_t func, void *arg)
	{
		int UNUSED_ATTR status;
		status = pthread_create(&thread, NULL, func, arg);
		assert(status == 0);
	}
	void stop()
	{
		int UNUSED_ATTR status = pthread_cancel(thread);
		assert(status == 0);
	}
	void exit(int status)
	{
		pthread_exit((void *) &status);
	}
	void join()
	{
		int UNUSED_ATTR status = pthread_join(thread, NULL);
		assert(status == 0);
	}
	bool isEqual(pthread_t other)
	{
		return pthread_equal(thread, other);
	}

};

}

#endif // !defined(KFS_THREAD_H)
