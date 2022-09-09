#ifndef BoundedBuffer_h
#define BoundedBuffer_h

#include <stdio.h>
#include <queue>
#include <string>
#include <pthread.h>
#include <thread>
#include <mutex>
#include <assert.h>
#include <condition_variable>

using namespace std;

class BoundedBuffer
{
private:
	int cap; // max number of items in the buffer
	queue<vector<char>> q;	/* the queue of items in the buffer. Note
	that each item a sequence of characters that is best represented by a vector<char> for 2 reasons:
	1. An STL std::string cannot keep binary/non-printables
	2. The other alternative is keeping a char* for the sequence and an integer length (i.e., the items can be of variable length).
	While this would work, it is clearly more tedious */

	// Add necessary synchronization variables and data structures 
	mutex m;
	condition_variable data_avail; // Wait for by pop, sig by push
	condition_variable slot_avail; // Wait for by push, sig by pop


public:
	BoundedBuffer(int _cap){
		cap = _cap;
		//cout << cap << endl;
	}

	~BoundedBuffer(){
		
	}

	void push(char* data, int len){
		//1. Convert the incoming byte sequence given by data and len into a vector<char>
		vector<char> idata (data, data + len);

		//2. Wait until there is room in the queue (i.e., queue lengh is less than cap)
		unique_lock<mutex> l (m);
		slot_avail.wait(l, [this]{return q.size() < cap;});

		//3. Then push the vector at the end of the queue. Watch for race condition.
		q.push(idata);
		l.unlock();

		//4. Wake up one thread and tell them its ok to read from buffer.
		data_avail.notify_one();

	}

	int pop(char* buf, int bufcap){
		//1. Wait until the queue has at least 1 item
		unique_lock<mutex> l (m);
		data_avail.wait(l, [this]{return q.size() > 0;}); 

		//2. pop the front item of the queue. The popped item is a vector<char>
		vector<char> rdata = q.front();
		q.pop();
		l.unlock();

		//3. Convert the popped vector<char> into a char*, copy that into buf, make sure that vector<char>'s length is <= bufcap
		assert(rdata.size() <= bufcap);
		memcpy(buf, rdata.data(), rdata.size()); // byte sequence access inside vector

		//4. Wake up
		slot_avail.notify_one();

		//5. Return the vector's length to the caller so that he knows many bytes were popped
		return rdata.size();

	}
};

#endif /* BoundedBuffer_ */
