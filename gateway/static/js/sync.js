// static/js/sync.js
class CartSyncManager {
    constructor() {
        this.isSyncing = false;
        this.pendingOperations = [];
        this.retryCount = 0;
        this.maxRetries = 3;
    }

    async syncCart(cartData, operation = 'update') {
        // Если уже идет синхронизация, добавляем операцию в очередь
        if (this.isSyncing) {
            this.pendingOperations.push({ cartData, operation });
            return;
        }

        this.isSyncing = true;

        try {
            await this._performSync(cartData, operation);
            this.retryCount = 0;

            // Обрабатываем ожидающие операции
            this.processPendingOperations();
        } catch (error) {
            console.error('Sync failed:', error);

            // Повторяем при необходимости
            if (this.retryCount < this.maxRetries) {
                this.retryCount++;
                setTimeout(() => this.syncCart(cartData, operation), 1000 * this.retryCount);
            }
        } finally {
            this.isSyncing = false;
        }
    }

    async _performSync(cartData, operation) {
        const response = await fetch('/sync_cart_state', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                cart: cartData,
                operation: operation
            })
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!data.success) {
            throw new Error(data.message || 'Sync failed');
        }

        // Обновляем локальное хранилище
        localStorage.setItem('fruitShopCart', JSON.stringify(data.cart));

        // Обновляем глобальную переменную
        if (window.cartManager) {
            window.cartManager.updateCartIndicator(data.cart_total);
        }

        return data;
    }

    processPendingOperations() {
        if (this.pendingOperations.length > 0) {
            const nextOp = this.pendingOperations.shift();
            this.syncCart(nextOp.cartData, nextOp.operation);
        }
    }

    // Периодическая синхронизация
    startPeriodicSync(interval = 30000) {
        this.periodicSync = setInterval(() => {
            const cartData = JSON.parse(localStorage.getItem('fruitShopCart') || '{}');
            if (Object.keys(cartData).length > 0) {
                this.syncCart(cartData, 'periodic');
            }
        }, interval);
    }

    stopPeriodicSync() {
        if (this.periodicSync) {
            clearInterval(this.periodicSync);
        }
    }
}

// Инициализация глобального синхронизатора
window.cartSyncManager = new CartSyncManager();