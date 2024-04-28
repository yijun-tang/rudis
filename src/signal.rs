use std::{mem::zeroed, ptr::null_mut};

use libc::{sigaction, sigemptyset, SA_NODEFER, SA_ONSTACK, SA_RESETHAND, SA_SIGINFO, SIGBUS, SIGFPE, SIGILL, SIGSEGV};

pub unsafe fn setup_sig_segv_action() {
    let mut act: sigaction;
    act = zeroed();

    sigemptyset(&mut act.sa_mask);
    // When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
    // is used. Otherwise, sa_handler is used
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
    // TODO: customized type for act.sa_sigaction
    // act.sa_sigaction = segv_handler;
    sigaction(SIGSEGV, &act, null_mut());
    sigaction(SIGBUS, &act, null_mut());
    sigaction(SIGFPE, &act, null_mut());
    sigaction(SIGILL, &act, null_mut());
}
