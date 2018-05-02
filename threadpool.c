#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>

#include "threadpool.h"

/*
 * A Work Stealing Pool
 * More information, please see: http://www.cnblogs.com/ok-wolf/p/7761755.html
 */
struct future
{
	fork_join_task_t task;
	void *arg; //parameter
	void *result;
	sem_t *sem;
	int status; //0: not to do, 1: doing, 2: done
	int local; //1: internal task, 0: external task
	struct future *prev;
	struct future *next;
};

struct thread_t 
{
	pthread_t id;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int idle;	//1: idle, 0: busy
	int index;  //record the current thread index in pool
	int current_task_num; //total task number in current thread
	struct thread_pool *pool; //point to the pool area
	struct future *head;
	struct future *tail;
};

struct thread_pool
{
	int max_threads;
	pthread_mutex_t mutex;
	int shutdown; //1: shutdown, 0: normal
	struct thread_t *threads;
	struct future *head;
	struct future *tail;
};


static void *thread_route(void *arg)
{
	assert(arg != NULL);
	struct thread_t *thread = (struct thread_t *)arg;
	assert(thread != NULL);
	struct thread_pool *pool = thread->pool;
	assert(pool != NULL);
	struct future *future = NULL;
	
	while(1)
	{	
		pthread_mutex_lock(&thread->mutex);
		if(future != NULL)
		{
			thread->idle = 0;
			future->status = 1; //doing
			future->result = future->task(pool, future->arg);
			future->status = 2;
			sem_post(future->sem);
		}

		while(thread->current_task_num == 0 && pool->shutdown == 0)
		{
			//wait for task assigment
			pthread_cond_wait(&thread->cond, &thread->mutex);
		}

		if(pool->shutdown == 1)
		{
			//pool is shutdown, destroy the local task list
			struct future *temp = NULL;
			while(thread->head != NULL)
			{
				temp = thread->head;
				thread->head = thread->head->next;
				free(temp);
			}	
			pthread_mutex_unlock(&thread->mutex);
			pthread_exit(NULL);
		}

		//Fist, get task from local task list to do
		while(thread->head != NULL)
		{
			thread->idle = 0;
			future = thread->head;
			thread->head = thread->head->next;
			if(thread->tail == future)
				thread->tail = NULL;
			else
				thread->head->prev = NULL;

			//call the callback to do work
			thread->current_task_num--;
			future->status = 1; //doing
			#if 0
			if(pool->max_threads == 1 && future->local == 1)
			{
				/*
				 * TBD: in case there is only a thread in pool 
				 * and the task is local task
				 * we can create a thread to do the task?
				 */
			}
			else
			#else
			{
				future->result = future->task(pool, future->arg);
			}
			#endif
			future->status = 2;
			sem_post(future->sem); //Let future_get know, the result is ok
		}
		pthread_mutex_unlock(&thread->mutex); 
		
		thread->idle = 1;
		
		/*
		 * The local task work are done, go to global task list to get task 
		 * or go to other work thread to get task.
		 */

		//Step1: Go to globacl task list to get task(From Head)
		pthread_mutex_lock(&pool->mutex);
		future = NULL;
		while(pool->head != NULL && pool->head->status == 0)
		{
			//printf("Worker %d get task from global task, current_task %d\n", thread->index, thread->current_task_num);
			future = pool->head;
			pool->head = pool->head->next;
			if(pool->tail == future)
				pool->tail = NULL;
			else
				pool->head->prev = NULL;

			//Get the future, then put into the local task list?
			#if 0
			pthread_mutex_lock(&thread->mutex);
			if(thread->head != NULL)
			{
				future->next = thread->head;
				thread->head->prev = future;
			}
			else
			{
				thread->tail = future;
			}
			thread->head = future;

			thread->current_task_num++;
	
			pthread_mutex_unlock(&thread->mutex);
			if(thread->current_task_num == 9)
			{
				//Get 10 tasks, ok, get out, give some changes to other work threads
				break;
			}
			#else
			//printf("Worked %d get one task from globack task list.\n", thread->index);
			break; //get one task, break
			#endif
		}
		pthread_mutex_unlock(&pool->mutex);

		//Step2: Go to other work thread task list to get task(From Tail)
		if(future == NULL && thread->current_task_num == 0)
		{
			//printf("Worker %d can not get task from global task, then try other work threads, current_task %d\n", thread->index, thread->current_task_num);
			future = pool->head;
			int i = 0;
			struct thread_t *other_thread = NULL;
			for(i=0; i<pool->max_threads; i++)
			{
				if(i == thread->index)
					continue; //myself

				if(pool->threads[i].current_task_num == 0)
					continue; //it has no task

				//lock it?
				pthread_mutex_lock(&pool->threads[i].mutex);
				other_thread = (struct thread_t *)&pool->threads[i];
				while(other_thread->tail != NULL && other_thread->tail->status == 0)
				{
					future = other_thread->tail;
					other_thread->tail = other_thread->tail->prev;
					if(future == other_thread->head)
						other_thread->head = NULL;
					else
						other_thread->tail->next = NULL;

					//Get the future, then put into our local task list?
					#if 0
					pthread_mutex_lock(&thread->mutex);
					if(thread->head != NULL)
					{
						future->next = thread->head;
						thread->head->prev = future;
					}
					else
					{
						thread->tail = future;
					}
					thread->head = future;

					thread->current_task_num++;
					printf("Worker %d get task from other thread task, current_task %d\n", thread->current_task_num);
					pthread_mutex_unlock(&thread->mutex);
					if(thread->current_task_num == 4)
					{
						//Get 4 tasks, ok, get out, give some changes to other work threads
						break;
					}
					#else
					//printf("Worked %d get one task from other worker %d.\n", thread->index, i);
					break; //get one task, break
					#endif
				}
				pthread_mutex_unlock(&pool->threads[i].mutex);
			}
		}
	}
}

struct thread_pool * thread_pool_new(int nthreads)
{
	struct thread_pool *pool = (struct thread_pool *)malloc(sizeof(struct thread_pool));
	assert(pool != NULL);

	pool->max_threads = nthreads;
	pool->head = pool->tail = NULL;

	pthread_mutex_init(&pool->mutex, NULL);

	pool->threads = (struct thread_t *)malloc(nthreads * sizeof(struct thread_t));
	assert(pool->threads != NULL);
	
	int i = 0;
	for(i=0; i<pool->max_threads; i++)
	{
		pthread_mutex_init(&pool->threads[i].mutex, NULL);
		pthread_cond_init(&pool->threads[i].cond, NULL);
		pool->threads[i].idle = 1; //idle
		pool->threads[i].index = i;
		pool->threads[i].pool = pool; //point to the pool area
		pool->threads[i].current_task_num = 0;
		pthread_create(&pool->threads[i].id, NULL, thread_route,(void *)(&pool->threads[i]));
	}	

	return pool;
}

struct future * thread_pool_submit(
        struct thread_pool *pool, 
        fork_join_task_t task, 
        void * data)
{
	assert(pool != NULL);
	struct future *future = (struct future *)malloc(sizeof(struct future));
	assert(future);

	future->task = task;
	future->arg = data;
	future->prev = future->next = NULL;
	future->result = NULL;
	future->status = 0;
	future->local = 0; //default is external task
	
	int i = 0;
	unsigned long myself_pid = pthread_self();
	for(i=0; i<pool->max_threads; i++)
	{
		if(pool->threads[i].id == myself_pid)
		{
			future->local = 1; //it is internal task
			break;
		}
	}
	
	future->sem = (sem_t *)malloc(sizeof(sem_t)); 
	assert(future->sem != NULL);
	sem_init(future->sem, 0, 0);
			
	//find a idle work thread to put the task
	struct thread_t * thread = NULL;
	for(i = 0; i< pool->max_threads; i++)
	{
		thread = &pool->threads[i];
		pthread_mutex_lock(&thread->mutex);
		if(thread->idle == 1)
		{
			//find it, insert the task from head
			if(thread->head != NULL)
			{
				future->next = thread->head;
				thread->head->prev = future;
			}
			else
			{
				thread->tail = future;
			}
			thread->head = future;

			thread->current_task_num++;
	
			//Just let work thread know, it has work to do
			if(thread->current_task_num == 1)
			{
				//printf("%s(): Let worker %d to start to work\n", __FUNCTION__, thread->index);
				pthread_cond_signal(&thread->cond);
			}
			pthread_mutex_unlock(&thread->mutex);

			return future;
		}
		pthread_mutex_unlock(&thread->mutex);
	}
	
	//can not find idle work thread, just put it into global task list
	//printf("%s(): no find idle work thread, just put into global task list\n", __FUNCTION__);			
	pthread_mutex_lock(&pool->mutex);
	if(pool->head != NULL)
	{
		future->next = pool->head;
		pool->head->prev = future;
	}
	else
	{
		pool->tail = future;
	}
	pool->head = future;
	pthread_mutex_unlock(&pool->mutex);

	return future;
}

void * future_get(struct future *future)
{
	assert(future);
	sem_wait(future->sem); //wait for the result ready
	return (void *)future->result;

}

void thread_pool_shutdown_and_destroy(struct thread_pool *pool)
{
	assert(pool != NULL);
	pool->shutdown = 1;

	//send signal to all work thread 
	int i = 0;
	for(i=0; i<pool->max_threads; i++)
	{
		pthread_cond_signal(&pool->threads[i].cond);
		pthread_join(pool->threads[i].id, NULL);
	}
	for(i=0; i<pool->max_threads; i++)
	{
		pthread_mutex_destroy(&pool->threads[i].mutex);
		pthread_cond_destroy(&pool->threads[i].cond);
	}
	free(pool->threads);
	
	pthread_mutex_lock(&pool->mutex);
	struct future *future = NULL;
	while(pool->head != NULL)
	{
		future = pool->head;
		pool->head = pool->head->next;
		free(future);
	}
	pthread_mutex_unlock(&pool->mutex);
	pthread_mutex_destroy(&pool->mutex);
	free(pool);
	pool = NULL;
}

void future_free(struct future *future)
{
	assert(future != NULL);
	sem_destroy(future->sem);
	free(future);
	future = NULL;
}

//----------------------------------------------------------------
#if 1
#define DEFAULT_THREADS 1

struct arg2 {
    int a;
    int b;
};


static void *
adder_task(struct thread_pool *pool, struct arg2 * data)
{
	//printf("%s: result: %d, a %d, b %d\n", __FUNCTION__, (data->a + data->b), data->a, data->b);
    return (void *)(data->a + data->b);
}



static int
run_test(int nthreads, long long unsigned max_num)
{
    struct thread_pool * threadpool = thread_pool_new(nthreads);

    struct arg2 args = {
        .a = 20,
        .b = 22,
    };

	struct future * sum  = NULL;
	long long unsigned ssum = 0;
	int i;
	for(i=1; i<max_num; i+=2)
	{
		args.a = i;
		args.b = i+1;
   		sum = thread_pool_submit(threadpool, (fork_join_task_t) adder_task, &args);
    	ssum += (long long unsigned) future_get(sum);
		future_free(sum);
	}
    
    thread_pool_shutdown_and_destroy(threadpool);
    
  	printf("max-num = %d, sum = %llu\n", max_num, ssum);
	
    return 0;
}

static void
usage(char *av0, int exvalue)
{
    fprintf(stderr, "Usage: %s [-n <n>]\n"
                    " -n number of threads in pool, default %d\n"
                    , av0, DEFAULT_THREADS);
    exit(exvalue);
}

int 
main(int ac, char *av[]) 
{
    int c, nthreads = DEFAULT_THREADS;
	long long unsigned max_num = 100;
    while ((c = getopt(ac, av, "hn:m:")) != EOF) {
        switch (c) {
        case 'n':
            nthreads = atoi(optarg);
            break;
		case 'm':
			max_num = atoll(optarg);
			break;
        case 'h':
            usage(av[0], EXIT_SUCCESS);
        }
    }

    return run_test(nthreads, max_num);
}
#endif

