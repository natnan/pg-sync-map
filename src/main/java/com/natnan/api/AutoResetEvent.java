package com.natnan.api;

// http://stackoverflow.com/a/6603137/846644
class AutoResetEvent {

  private final Object monitor = new Object();
  private volatile boolean open = false;

  public AutoResetEvent(boolean open) {
    this.open = open;
  }

  public void waitOne() throws InterruptedException {
    synchronized (monitor) {
      while (!open) {
        monitor.wait();
      }
      open = false; // close for other
    }
  }

  public void set() {
    synchronized (monitor) {
      open = true;
      monitor.notify(); // open one
    }
  }

  public void reset() { //close stop
    open = false;
  }
}
