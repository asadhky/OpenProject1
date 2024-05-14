#include "OLC_BTree.hpp"
#include <iostream>

// -------------------------------------------------------------------------------------
// BTREE NODES
// -------------------------------------------------------------------------------------
unsigned BTreeLeaf::lowerBound(Key k) {
    unsigned l=0;
    unsigned r=count;
    while (l < r) {
        unsigned mid = l+(r-l)/2;
        if (keys[mid] < k) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }
    return l;
}

void BTreeLeaf::insert(Key k, Payload p) {
    bool restart = false;
    while (true) {
        writeLockOrRestart(restart);
        if (!restart) {
            insertInLeaf(k, p);
            writeUnlock();
            return;
        }
    }
}

void BTreeLeaf::insertInLeaf(Key k, Payload p) {
    bool restart = false;
    while (true) {
        writeLockOrRestart(restart);
        if (!restart) {
            insertAndSplitIfNeeded(k, p);
            writeUnlock();
            return;
        }
    }
}

void BTreeLeaf::insertAndSplitIfNeeded(Key k, Payload p) {
    if (isFull()) {
        Key sep;
        BTreeLeaf* newleaf = split(sep);
        if (k >= sep) {
            newleaf->insertInLeaf(k, p);
        } else {
            insert(k, p);
        }
    } else {
        insert(k, p);
    }
}

BTreeLeaf* BTreeLeaf::split(Key& sep) {
    bool restart = false;
    while (true) {
        writeLockOrRestart(restart);
        if (!restart) {
            BTreeLeaf* newleaf = new BTreeLeaf();
            unsigned mid = count / 2;
            sep = keys[mid];
            for (unsigned i = mid; i < count; ++i) {
                newleaf->keys[i - mid] = keys[i];
                newleaf->payloads[i - mid] = payloads[i];
            }

            newleaf->count = count - mid;
            count = mid;
            writeUnlock();
            return newleaf;
        }
    }
}

// -------------------------------------------------------------------------------------
unsigned BTreeInner::lowerBound(Key k) {
    unsigned l = 0;
    unsigned r = count;
    while (l < r) {
        unsigned mid = l + (r - l) / 2;
        if (keys[mid] < k) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }
    return l;
}

BTreeInner* BTreeInner::split(Key& sep) {
    bool restart = false;
    while (true) {
        writeLockOrRestart(restart);
        if (!restart) {
            BTreeInner* inner = new BTreeInner();
            unsigned mid = count / 2;
            sep = keys[mid];

            for (unsigned i = mid + 1; i < count; ++i) {
                inner->keys[i - mid - 1] = keys[i];
                inner->children[i - mid - 1] = children[i];
            }

            inner->count = count - mid - 1;
            count = mid + 1;
            writeUnlock();
            return inner;
        }
    }
}

void BTreeInner::insert(Key k, NodeBase* child) {
    bool restart = false;
    while (true) {
        writeLockOrRestart(restart);
        if (!restart) {
            unsigned j = lowerBound(k);
            for (unsigned i = count; i > j; --i) {
                keys[i] = keys[i - 1];
                children[i] = children[i - 1];
            }

            keys[j] = k;
            children[j] = child;
            ++count;
            writeUnlock();
            return;
        }
    }
}

// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
// BTREE 
// -------------------------------------------------------------------------------------
void OLC_BTree::upsert(Key k, Payload v) {
    bool restart = false;
    while (true) {
        uint64_t starttheread = root.load()->readLockOrRestart(restart);
        NodeBase* curr = root.load();
        NodeBase* parents = nullptr;

        while (curr->type != NodeType::BTreeLeaf) {
            BTreeInner* inner = static_cast<BTreeInner*>(curr);
            unsigned j = inner->lowerBound(k);
            parents = curr;
            curr = inner->children[j];
            curr->readLockOrRestart(restart);
            if (restart) break;
            parents->readUnlockOrRestart(starttheread, restart);
            if (restart) break;
            starttheread = curr->latchVersion.load();
        }

        if (!restart) {
            BTreeLeaf* leaf = static_cast<BTreeLeaf*>(curr);
            leaf->writeLockOrRestart(restart);
            if (!restart) {
                unsigned j = leaf->lowerBound(k);
                if (j < leaf->count && leaf->keys[j] == k) {
                    leaf->payloads[j] = v;
                } else {
                    leaf->insert(k, v);
                }
                leaf->writeUnlock();
            }
            leaf->readUnlockOrRestart(starttheread, restart);
        }

        curr->readUnlockOrRestart(starttheread, restart);
        if (!restart) {
            return;
        }
        std::cout << "Restarting upsert function..." << std::endl;
    }
}

bool OLC_BTree::lookup(Key k, Payload& result) {
    bool restart = false;
    while (true) {
        uint64_t starttheread = root.load()->readLockOrRestart(restart);
        NodeBase* curr = root.load();

        while (curr->type != NodeType::BTreeLeaf) {
            BTreeInner* inner = static_cast<BTreeInner*>(curr);
            unsigned j = inner->lowerBound(k);
            curr = inner->children[j];
            curr->readLockOrRestart(restart);
            if (restart) break;
            curr->readUnlockOrRestart(starttheread, restart);
            if (restart) break;
            starttheread = curr->latchVersion.load();
        }

        if (!restart) {
            BTreeLeaf* leaf = static_cast<BTreeLeaf*>(curr);
            leaf->readLockOrRestart(restart);
            if (!restart) {
                unsigned j = leaf->lowerBound(k);
                if (j < leaf->count && leaf->keys[j] == k) {
                    result = leaf->payloads[j];
                    leaf->readUnlockOrRestart(starttheread, restart);
                    curr->readUnlockOrRestart(starttheread, restart);
                    return true;
                }
                leaf->readUnlockOrRestart(starttheread, restart);
            }
            curr->readUnlockOrRestart(starttheread, restart);
        }

        if (!restart) {
            return false;
        }
    }
}

void OLC_BTree::makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild) {
    bool restart = false;
    while (true) {
        uint64_t starttheread = root.load()->readLockOrRestart(restart);
        BTreeInner* newroot = new BTreeInner();
        newroot->insert(k, leftChild);
        newroot->insert(k, rightChild);

        root.load()->writeLockOrRestart(restart);
        if (!restart) {
            root.store(newroot);
            root.load()->writeUnlock();
            root.load()->readUnlockOrRestart(starttheread, restart);
            return;
        }

        delete newroot;
        root.load()->readUnlockOrRestart(starttheread, restart);
    }
}

// int main() {
//     OLC_BTree btree;

//     btree.upsert(10, 100);
//     btree.upsert(20, 200);
//     btree.upsert(15, 150);

//     Payload result;
//     if (btree.lookup(20, result)) {
//         std::cout << "Value for key 20: " << result << std::endl;
//     } else {
//         std::cout << "Key 20 not found." << std::endl;
//     }

//     btree.upsert(25, 250);

//     std::cout << "Tree height: " << btree.getHeight() << std::endl;

//     return 0;
// }
