#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "TCPreqchannel.h"
#include <thread>
#include <signal.h>
#include <time.h>
#include <sys/epoll.h>

#define CLOCKID CLOCK_REALTIME
#define SIG SIGRTMIN
#define errExit(msg)    do { perror(msg); exit(EXIT_FAILURE); } while (0)

using namespace std;

//CHRISTOPHER PLUMMER - 512.
//Help from the linux man page for timer_create() - > https://man7.org/linux/man-pages/man2/timer_create.2.html

void patient_thread_function(int n, int pnum, BoundedBuffer* rb){
    /* What will the patient threads do? */
    datamsg d (pnum, 0.000, 1); 

    for(int i = 0; i < n; i++) {
        rb->push((char *) &d, sizeof(d));
        d.seconds += 0.004;
    }   
}

void file_thread_function(string fname, BoundedBuffer* rb, TCPRequestChannel* chan, int caplen){
    
    // 1. Create file of same length
    int to_alloc = sizeof (filemsg) + fname.size() + 1; // extra byte for NULL
    char buf [to_alloc];
    filemsg f (0,0);
    memcpy (buf, &f, sizeof(f));
    strcpy (buf + sizeof(f), fname.c_str());
    chan->cwrite(buf, to_alloc);
    __int64_t filelen;
    chan->cread(&filelen, sizeof(__int64_t));
    std::cout << "File size: " << filelen << endl;

    string recvfname = string("received/") + fname;
    FILE* fp = fopen(recvfname.c_str(), "w");
    fseek(fp, filelen, SEEK_SET); //Set file to file length
    fclose(fp);

    // 2. Generate all file messages and push onto bounded buffer.
    filemsg* fm = (filemsg * ) buf;
    __int64_t rem = filelen;
    fm->offset = 0;

    while (rem > 0){
        //cout << rem << endl;
        fm->length = min (rem, (__int64_t) caplen);
        rb->push(buf, sizeof(filemsg) + fname.size() + 1);
        rem -= fm->length;
        fm->offset += fm->length;
    }
}

void event_polling_function(int n, int p, int w, int mb, TCPRequestChannel** wchans, BoundedBuffer* request_buffer, HistogramCollection* hc){

    /* Functionality of the worker threads*/
    char buf [1024];
    double response = 0;
    char recv_buffer [mb];
    struct epoll_event ev, events[w];


    // Create epoll list
    int epollfd = epoll_create1 (0);
    if (epollfd == -1){
        EXITONERROR ("epoll_create1");
    }

    unordered_map<int, int> fd_to_index;
    vector<vector<char>> state (w);

    // prime channels & each fd added to list
    bool quit_recv = false;
    int nsent = 0, nrecv = 0;

    for(int i=0; i<w; i++){
        int sz = request_buffer->pop(buf, 1024);
        if(*(MESSAGE_TYPE*)buf == QUIT_MSG){ //Fix deadlock when w > n * p
            quit_recv = true;
            break; 
        }

        wchans[i]->cwrite(buf, sz);
        state[i] = vector<char>(buf, buf+sz);
        nsent++;
        int rfd = wchans[i]->getSockFD();
        fcntl(rfd, F_SETFL, O_NONBLOCK);

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = rfd;
        fd_to_index [rfd] = i;
        if(epoll_ctl(epollfd, EPOLL_CTL_ADD, rfd, &ev) == -1){
            EXITONERROR("epoll_ctl: listen_sock");
        }
    }
    cout << "Here Primed Channels" << endl;

    // wait
    while(true){

        if(quit_recv == true && nsent == nrecv){ break; }

        int nfds = epoll_wait(epollfd, events, w, -1);
        if(nfds == -1){
            EXITONERROR ("epoll_wait");
        }

        for(int i = 0; i < nfds; i++) {
            int rfd = events[i].data.fd;
            int index = fd_to_index [rfd];

            int resp_sz = wchans[index]->cread(recv_buffer, mb);
            nrecv++;

            // process recv_buffer
            vector<char> req = state[index];
            char* request = req.data();

            // process response
            MESSAGE_TYPE* m = (MESSAGE_TYPE *) request;
            if (*m == DATA_MSG) {

                hc->update(((datamsg *)request)->person, *(double*)recv_buffer);

            } else if (*m == FILE_MSG){

                filemsg* f = (filemsg*)request;
                string fname = (char *)(f + 1);
                int sz = sizeof(filemsg) + fname.size() + 1;

                string recvfname = "received/" + fname;
                FILE* fp = fopen(recvfname.c_str(), "r+");
                fseek(fp, f->offset, SEEK_SET);
                fwrite(recv_buffer, 1, f->length, fp);
                fclose(fp);
            }

            // reuse channel
            if(!quit_recv){
                int req_sz = request_buffer->pop(buf, sizeof(buf));
                if(*(MESSAGE_TYPE*)buf == QUIT_MSG){
                    quit_recv = true;
                } else {
                    wchans[index]->cwrite(buf, req_sz);
                    state[index] = vector<char> (buf, buf+req_sz);
                    nsent ++;
                }
            }
        }
    }
    cout << "Here Done While" << endl;
    //delete recv_buffer;
}



int main(int argc, char *argv[])
{
    int n = 15000;  // default number of requests per "patient"
    int p = 15;     // number of patients [1,15]
    int w = 100;    // default number of worker threads
    int b = 500; 	// default capacity of the request buffer, you should change this default
	int m = MAX_MESSAGE; 	// default capacity of the message buffer
    srand(time_t(NULL));
    bool isfiletransfer = false;
    string fname;
    string host;
    string port;

    int c = -1;
    
    while ((c = getopt (argc, argv, "m:n:b:w:f:p:h:r:")) != -1){
        switch (c){
            case 'm':
                m = atoi (optarg);
                break;

            case 'n':
                n = atoi (optarg);
                break;

            case 'b':
                b = atoi (optarg);
                break;

            case 'w':
                w = atoi (optarg);
                break;

            case 'p':
                p = atoi(optarg);
                break;

            case 'f':
                isfiletransfer = true;
                fname = optarg;
                break;

            case 'h':
                host = optarg;
                break;

            case 'r':
                port = optarg;
                break;

        }
    }

    
	
    BoundedBuffer request_buffer(b);
	HistogramCollection hc;
	
    //Create p number of histograms. Add them to hist collection.
    for(int i = 0; i < p; i++){
        Histogram* h = new Histogram(10, -2.0, 2.0);
        hc.add(h);
    }
	

    // Worker channel creation 1 per w.
    TCPRequestChannel** wchans = new TCPRequestChannel* [w];
    for(int i = 0; i < w; ++i){
        wchans[i] = new TCPRequestChannel(host, port);
    }
    cout << "Created TCP Worker Channels" << endl;

	
    /* Timed Section Start*/ 
    struct timeval start, end;
    double elapse;
    gettimeofday(&start, 0); // Start timer for 1k points through all active channels.

    thread pthreads[p];

    if(!isfiletransfer){ //DATA TRANSFER

        //Start Threads
        for(int i = 0; i < p; i++){
            pthreads[i] = thread(patient_thread_function, n, i+1, &request_buffer);
        }
        cout << "Created Patient Threads Channels" << endl;

        thread evp (event_polling_function, n, p, w, m, wchans, &request_buffer, &hc); //Create function
        cout << "Created Worker Thread" << endl;
        
        
        //Join Threads
        for(int i = 0; i < p; i++){
            pthreads[i].join();
        }
        cout << "Joined Patient Threads" << endl;

        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char *) &q, sizeof(q));
        evp.join();
        cout << "Joined evp"<< endl;

    } else { //FILE TRANSFER

  	TCPRequestChannel* file_channel = new TCPRequestChannel(host, port);
        
	//Start Threads
	
        thread filethread(file_thread_function, fname, &request_buffer, file_channel, m);
        cout << "Created File Threads Channels" << endl;

        thread evp (event_polling_function, n, p, w, m, wchans, &request_buffer, &hc); //Create function
        cout << "Created Worker Thread" << endl;

        //Join Threads
        filethread.join();
        cout << "File Thread Done" << endl;

        MESSAGE_TYPE q = QUIT_MSG;
        request_buffer.push((char *) &q, sizeof(q));
        evp.join();
        cout << "Joined evp"<< endl;
	delete file_channel;
    }

    gettimeofday (&end, 0);

    // Print all histograms and time results.
	hc.print ();

    int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
    int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
    std::cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

    elapse = (end.tv_sec - start.tv_sec) * 1e6;
    elapse = (elapse + (end.tv_usec -  start.tv_usec)) * 1e-6;
    std::cout << "Took " << fixed << elapse << setprecision(6); 
    std::cout << " seconds." << endl;

    /* Timed Section End*/

    //Clean up heap

    MESSAGE_TYPE q = QUIT_MSG;
    
    for(int i = 0; i < w; ++i){
        wchans[i]->cwrite(&q, sizeof(MESSAGE_TYPE));
        delete wchans[i];
    }

    delete[] wchans; //[]?

    std::cout << "All Done!!!" << endl;
}
