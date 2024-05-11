#include "OLC_BTree.hpp"

// -------------------------------------------------------------------------------------
// BTREE NODES
// -------------------------------------------------------------------------------------
unsigned BTreeLeaf::lowerBound(Key k) {
    // Binary search to find the index where the key should be inserted
    unsigned left = 0;
    unsigned right = count;
    while (left < right) {
        unsigned mid = left + (right - left) / 2;
        if (keys[mid] < k) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

void BTreeLeaf::insert(Key k, Payload p) {
    bool needRestart = false;
    while (true) {
        // Start with write lock on the current leaf node
        writeLockOrRestart(needRestart);
        if (!needRestart) {
            // Attempt to insert the key-value pair into the leaf node
            insertInLeaf(k, p);
            // Release write lock on the current leaf node
            writeUnlock();
            return;
        }
        // If restart is needed, retry the operation
    }
}

void BTreeLeaf::insertInLeaf(Key k, Payload p) {
    bool needRestart = false;
    while (true) {
        // Start with write lock on the current leaf node
        writeLockOrRestart(needRestart);
        if (!needRestart) {
            // Attempt to insert the key-value pair into the leaf node
            insertAndSplitIfNeeded(k, p);
            // Release write lock on the current leaf node
            writeUnlock();
            return;
        }
        // If restart is needed, retry the operation
    }
}

void BTreeLeaf::insertAndSplitIfNeeded(Key k, Payload p) {
    // If the leaf node is full, split it and insert the key-value pair
    if (isFull()) {
        Key sep;
        BTreeLeaf* newLeaf = split(sep);
        if (k >= sep) {
            newLeaf->insertInLeaf(k, p);
        } else {
            insert(k, p);
        }
    } else {
        insert(k, p);
    }
}


BTreeLeaf* BTreeLeaf::split(Key& sep) {
    bool needRestart = false;
    while (true) {
        // Start with write lock on the current leaf node
        writeLockOrRestart(needRestart);
        if (!needRestart) {
            // Create a new leaf node for the right split part
            BTreeLeaf* newLeaf = new BTreeLeaf();

            // Calculate the mid index for splitting
            unsigned mid = count / 2;

            // Set the separation key
            sep = keys[mid];

            // Copy keys and payloads to the new leaf node
            for (unsigned i = mid; i < count; ++i) {
                newLeaf->keys[i - mid] = keys[i];
                newLeaf->payloads[i - mid] = payloads[i];
            }

            // Update count for both nodes
            newLeaf->count = count - mid;
            count = mid;

            // Release write lock on the current leaf node
            writeUnlock();

            return newLeaf;
        }
        // If restart is needed, retry the operation
    }
}


// -------------------------------------------------------------------------------------
unsigned BTreeInner::lowerBound(Key k) {
    // Binary search to find the child index where the key should go
    unsigned left = 0;
    unsigned right = count;
    while (left < right) {
        unsigned mid = left + (right - left) / 2;
        if (keys[mid] < k) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}


BTreeInner* BTreeInner::split(Key& sep) {
    bool needRestart = false;
    while (true) {
        // Start with write lock on the current inner node
        writeLockOrRestart(needRestart);
        if (!needRestart) {
            // Create a new inner node for the right split part
            BTreeInner* newInner = new BTreeInner();

            // Calculate the mid index for splitting
            unsigned mid = count / 2;

            // Set the separation key
            sep = keys[mid];

            // Copy keys and children to the new inner node
            for (unsigned i = mid + 1; i < count; ++i) {
                newInner->keys[i - mid - 1] = keys[i];
                newInner->children[i - mid - 1] = children[i];
            }

            // Update counts for both nodes
            newInner->count = count - mid - 1;
            count = mid + 1;

            // Release write lock on the current inner node
            writeUnlock();

            return newInner;
        }
        // If restart is needed, retry the operation
    }
}

void makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild);

void BTreeInner::insert(Key k, NodeBase* child) {
    bool needRestart = false;
    while (true) {
        // Start with write lock on the current inner node
        writeLockOrRestart(needRestart);
        if (!needRestart) {
            // Find the position where the key should be inserted
            unsigned idx = lowerBound(k);

            // Shift keys and children to make space for the new key and child
            for (unsigned i = count; i > idx; --i) {
                keys[i] = keys[i - 1];
                children[i] = children[i - 1];
            }

            // Insert the key and child at the correct position
            keys[idx] = k;
            children[idx] = child;

            // Increment the count
            ++count;

            // Release write lock on the current inner node
            writeUnlock();

            return;
        }
        // If restart is needed, retry the operation
    }
}

// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
// BTREE 
// -------------------------------------------------------------------------------------
void OLC_BTree::upsert(Key k, Payload v) {
    bool needRestart = false;
    while (true) {
        // Start with read lock on root
        uint64_t startRead = root.load()->readLockOrRestart(needRestart);

        // Variables to keep track of the current node and its parent
        NodeBase* currNode = root.load();
        NodeBase* parentNode = nullptr;

        // Traverse the tree to find the leaf node where the key should be inserted or updated
        while (currNode->type != NodeType::BTreeLeaf) {
            BTreeInner* innerNode = static_cast<BTreeInner*>(currNode);
            // Find the child index where the key should go
            unsigned idx = innerNode->lowerBound(k);
            parentNode = currNode;
            currNode = innerNode->children[idx];
            // Acquire read lock on the child node
            currNode->readLockOrRestart(needRestart);
            if (needRestart) break;
            // Release read lock on the parent node
            parentNode->readUnlockOrRestart(startRead, needRestart);
            if (needRestart) break;
            // Update startRead for parent node
            startRead = currNode->latchVersion.load();
        }

        if (!needRestart) {
            // We've reached the leaf node where the key should be inserted or updated
            BTreeLeaf* leafNode = static_cast<BTreeLeaf*>(currNode);
            // Acquire write lock on the leaf node
            leafNode->writeLockOrRestart(needRestart);
            if (!needRestart) {
                // Perform upsert operation in the leaf node
                unsigned idx = leafNode->lowerBound(k);
                if (idx < leafNode->count && leafNode->keys[idx] == k) {
                    // Key already exists, update the value
                    leafNode->payloads[idx] = v;
                } else {
                    // Key doesn't exist, insert the key-value pair
                    leafNode->insert(k, v);
                }
                // Release write lock on the leaf node
                leafNode->writeUnlock();
            }
            // Release read lock on the leaf node
            leafNode->readUnlockOrRestart(startRead, needRestart);
        }

        // Release read lock on the root node
        currNode->readUnlockOrRestart(startRead, needRestart);

        // If no restart is needed, return
        if (!needRestart) {
            return;
        }
        // If restart is needed, retry the operation
    }
}

bool OLC_BTree::lookup(Key k, Payload& result) {
    bool needRestart = false;
    while (true) {
        // Start with read lock on root
        uint64_t startRead = root.load()->readLockOrRestart(needRestart);

        // Variables to keep track of the current node
        NodeBase* currNode = root.load();

        // Traverse the tree to find the leaf node where the key should be located
        while (currNode->type != NodeType::BTreeLeaf) {
            BTreeInner* innerNode = static_cast<BTreeInner*>(currNode);
            // Find the child index where the key should go
            unsigned idx = innerNode->lowerBound(k);
            // Move to the child node
            currNode = innerNode->children[idx];
            // Acquire read lock on the child node
            currNode->readLockOrRestart(needRestart);
            if (needRestart) break;
            // Release read lock on the parent node
            currNode->readUnlockOrRestart(startRead, needRestart);
            if (needRestart) break;
            // Update startRead for parent node
            startRead = currNode->latchVersion.load();
        }

        if (!needRestart) {
            // We've reached the leaf node where the key should be located
            BTreeLeaf* leafNode = static_cast<BTreeLeaf*>(currNode);
            // Acquire read lock on the leaf node
            leafNode->readLockOrRestart(needRestart);
            if (!needRestart) {
                // Search for the key in the leaf node
                unsigned idx = leafNode->lowerBound(k);
                if (idx < leafNode->count && leafNode->keys[idx] == k) {
                    // Key found, update result and return true
                    result = leafNode->payloads[idx];
                    // Release read lock on the leaf node
                    leafNode->readUnlockOrRestart(startRead, needRestart);
                    // Release read lock on the root node
                    currNode->readUnlockOrRestart(startRead, needRestart);
                    return true;
                }
                // Key not found, return false
                // Release read lock on the leaf node
                leafNode->readUnlockOrRestart(startRead, needRestart);
            }
            // Release read lock on the root node
            currNode->readUnlockOrRestart(startRead, needRestart);
        }

        // If no restart is needed, return false
        if (!needRestart) {
            return false;
        }
        // If restart is needed, retry the operation
    }
}


void OLC_BTree::makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild) {
    bool needRestart = false;
    while (true) {
        // Start with read lock on root
        uint64_t startRead = root.load()->readLockOrRestart(needRestart);

        // Create a new inner node to serve as the root
        BTreeInner* newRoot = new BTreeInner();
        newRoot->insert(k, leftChild);
        newRoot->insert(k, rightChild);

        // Attempt to acquire write lock on the current root node
        root.load()->writeLockOrRestart(needRestart);
        if (!needRestart) {
            // Replace the current root node with the new root node
            root.store(newRoot);
            // Release write lock on the current root node
            root.load()->writeUnlock();
            // Release read lock on the root node
            root.load()->readUnlockOrRestart(startRead, needRestart);
            // Return as the operation is successful
            return;
        }

        // If restart is needed, release locks and retry the operation
        delete newRoot;
        // Release read lock on the root node
        root.load()->readUnlockOrRestart(startRead, needRestart);
    }
}
