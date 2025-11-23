package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Iterator;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if(requestType == LockType.NL) return;
        LockType intentLock;
        if(requestType == LockType.X){
            intentLock = LockType.IX;
        }else{
            intentLock = LockType.IS;
        }
        if(effectiveLockType == LockType.NL){
            if(explicitLockType == LockType.NL){
                getIntentLocks(transaction, parentContext, intentLock);
                lockContext.acquire(transaction, requestType);
            }else{
                if(explicitLockType == LockType.IX && requestType == LockType.S){
                    lockContext.promote(transaction, LockType.SIX);
                }else if(explicitLockType == LockType.IS && requestType == LockType.X){
                    getIntentLocks(transaction, parentContext, intentLock);
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, requestType);
                }else{
                    // IS -> S or IX -> X
                    lockContext.escalate(transaction);
                }
            }
        }else{
           if(effectiveLockType == LockType.X || effectiveLockType == requestType){
               return;
           }
           // effectiveLockType is S, requestType is X
           if(explicitLockType == LockType.NL){
               getIntentLocks(transaction, parentContext, intentLock);
               lockContext.acquire(transaction, requestType);
           }else{
               if(explicitLockType == LockType.S){
                   getIntentLocks(transaction, parentContext, intentLock);
                   lockContext.promote(transaction, requestType);
               }else if(explicitLockType == LockType.SIX){
                   lockContext.promote(transaction, requestType);
               }else if(explicitLockType == LockType.IX){
                   lockContext.escalate(transaction);
               }else if(explicitLockType == LockType.IS){
                   // ancestor held SIX, current ctx don't held IS
               }
           }
        }
        return;
    }


    // TODO(proj4_part2) add any helper methods you want
    private  static  void getIntentLocks(TransactionContext trans,  LockContext parentContext,
                                         LockType intentLock){
        if(parentContext == null){
            return;
        }
        Iterator<String> names = parentContext.name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = parentContext.lockman.context(n1);
        getIntentLockOnAncestor(trans, ctx, intentLock);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
            getIntentLockOnAncestor(trans, ctx, intentLock);
        }
    }

    private  static  void getIntentLockOnAncestor(TransactionContext trans, LockContext ancestor,
                                                  LockType intentLock ){
        LockType ancestorLockType = ancestor.lockman.getLockType(trans, ancestor.name);
        if(ancestorLockType == LockType.NL){
            ancestor.acquire(trans, intentLock);
        }else{
            if(!LockType.substitutable(ancestorLockType, intentLock)){
                if(ancestorLockType == LockType.S && intentLock == LockType.IX){
                    ancestor.promote(trans, LockType.SIX);
                }else{
                    ancestor.promote(trans, intentLock);
                }
            }
        }
    }
}
