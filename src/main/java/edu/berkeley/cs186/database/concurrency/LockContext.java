package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if(readonly){
            throw   new UnsupportedOperationException("the context is readonly!");
        }
        LockType curLockType = lockman.getLockType(transaction, name);
        if(curLockType != LockType.NL){
            throw   new DuplicateLockRequestException("lock existed");
        }
        if(lockType == LockType.NL){
            throw   new InvalidLockException("don't acquire NL lock");
        }
        if(parent == null){
            lockman.acquire(transaction, name, lockType);
        }else{
            if((lockType == LockType.IS || lockType == LockType.S) && hasSIXAncestor(transaction)){
                throw new InvalidLockException("prohibit acquiring an IS/S lock if an ancestor has SIX,");
            }
            LockType parentLockType = lockman.getLockType(transaction, parent.name);
            if(LockType.canBeParentLock(parentLockType, lockType)){
                lockman.acquire(transaction, name, lockType);
                Long transNum = transaction.getTransNum();
                parent.numChildLocks.put(transNum, parent.numChildLocks.getOrDefault(transNum, 0) + 1);
            }else{
                throw new InvalidLockException(" multi-granularity constraint is not  met.");
            }
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if(readonly){
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType curLockType = lockman.getLockType(transaction, name);
        if(curLockType == LockType.NL){
            throw new NoLockHeldException("LockContext release failed!");
        }
        Long transNum = transaction.getTransNum();
        if(getNumChildren(transaction) == 0){
            lockman.release(transaction,name);
            if(parent != null){
                parent.numChildLocks.put(transNum, parent.numChildLocks.get(transNum) - 1);
            }
        }else{
            throw new InvalidLockException(" LockContext release: violate multi-granularity locking constraints");
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if(readonly){
            throw new UnsupportedOperationException("the context is readonly");
        }
        LockType curLockType = lockman.getLockType(transaction, name);
        if(curLockType == LockType.NL){
            throw new NoLockHeldException("LockContext upgrade a non-existent lock");
        }
        if(curLockType == newLockType){
            throw new DuplicateLockRequestException("LockContext promote to the same type of  lock!");
        }
        if(newLockType == LockType.NL){
            throw new InvalidLockException("don't promote to NL");
        }
        if(LockType.substitutable(newLockType, curLockType)){
            if(parent != null){
                LockType parentLockType = lockman.getLockType(transaction, parent.name);
                if(!LockType.canBeParentLock(parentLockType, newLockType)){
                    throw new InvalidLockException("LockContext promote: violate multi-granularity locking " +
                            "constraints");
                }
            }
            if(newLockType == LockType.SIX){
                if(hasSIXAncestor(transaction)){
                    throw new InvalidLockException("disallow promotion to a SIX lock if an ancestor has SIX");
                }
                List<ResourceName> releaseNames = sisDescendants(transaction);
                releaseNames.add(name);
                lockman.acquireAndRelease(transaction,name, newLockType, releaseNames);
                for(ResourceName releaseName : releaseNames){
                    if(releaseName ==  name) {
                        continue;
                    }
                    LockContext ctx = fromResourceName(lockman, releaseName);
                    Map<Long, Integer>  numChildLocksParent =  ctx.parent.numChildLocks;
                    Long transNum = transaction.getTransNum();
                    numChildLocksParent.put(transNum, numChildLocksParent.get(transNum) - 1);
                }

            }else{
                lockman.promote(transaction, name, newLockType);
            }

        }
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if(readonly){
            throw new  UnsupportedOperationException("readonly!");
        }
        LockType curLockType = lockman.getLockType(transaction, name);
        if(curLockType == LockType.NL){
            throw new NoLockHeldException("escalate: has no lock at this level");
        }
        if(curLockType == LockType.S || curLockType == LockType.X){
            return;
        }
        boolean upgradeToX = false;
        List<ResourceName> descendantsHeldLock = new ArrayList<>();
        Queue<LockContext> q = new LinkedList<>();
        q.offer(this);
        while(!q.isEmpty()){
            LockContext node = q.poll();
            LockType lockTypeOfNode = lockman.getLockType(transaction, node.name);
            if(lockTypeOfNode != LockType.NL){
                descendantsHeldLock.add(node.name);
                if(!LockType.substitutable(LockType.S, lockTypeOfNode)){
                    upgradeToX = true;
                }
                for(LockContext child : node.children.values()){
                    q.offer(child);
                }
            }

        }
        if(upgradeToX){
            lockman.acquireAndRelease(transaction, name, LockType.X, descendantsHeldLock);
        }else{
            lockman.acquireAndRelease(transaction, name, LockType.S, descendantsHeldLock);
        }
        for(ResourceName releaseName : descendantsHeldLock){
            if(releaseName ==  name) {
                continue;
            }
            LockContext ctx = fromResourceName(lockman, releaseName);
            Map<Long, Integer>  numChildLocksParent =  ctx.parent.numChildLocks;
            Long transNum = transaction.getTransNum();
            numChildLocksParent.put(transNum, numChildLocksParent.get(transNum) - 1);
        }
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType curType = lockman.getLockType(transaction, name);
        LockContext curParent = parent;
        while(curType == LockType.NL && curParent != null){
            curType = lockman.getLockType(transaction, curParent.name);
            curParent =curParent.parent;
        }
        if(curType == LockType.SIX || curType == LockType.S){
            return LockType.S;
        }else if(curType == LockType.X){
            return LockType.X;
        }else{
            return LockType.NL;
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
       LockContext curParent = parent;
       while(curParent != null){
           LockType curParentLockType = lockman.getLockType(transaction, curParent.name);
           if (curParentLockType == LockType.SIX){
               return true;
           }else{
               curParent = curParent.parent;
           }
       }
       return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> res = new ArrayList<>();
        Queue<LockContext> q = new LinkedList<>();
        q.offer(this);
        while(!q.isEmpty()){
            LockContext node = q.poll();
            LockType lockTypeOfNode = lockman.getLockType(transaction, node.name);
            if(node != this){
                if(lockTypeOfNode == LockType.S || lockTypeOfNode == LockType.IS){
                    res.add(node.name);
                }
            }
            if(lockTypeOfNode != LockType.NL){
                for(LockContext child : node.children.values()){
                    q.offer(child);
                }
            }

        }
        return res;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

