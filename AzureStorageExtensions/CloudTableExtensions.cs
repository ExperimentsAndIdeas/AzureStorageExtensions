using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace makerofthings7.AzureStorage
{
    public static class CloudTableExtensions
    {
        public static Task CreateAsync(this CloudTable tbl, CancellationToken token)
        {
            return tbl.CreateAsync(null, null, token);
        }

        public static Task CreateAsync(this CloudTable tbl, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginCreate(null, null);
            else
                result = tbl.BeginCreate(opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                try
                {
                    tbl.EndCreate(ar);
                }
                catch (StorageException se)
                {
                    // Todo: Use the PnP azure retry block here for transient exceptions
                    throw;
                }
            });
        }

        public static Task<bool> CreateIfNotExistsAsync(this CloudTable tbl, CancellationToken token)
        {
            return tbl.CreateIfNotExistsAsync(null, null, token);
        }

        public static Task<bool> CreateIfNotExistsAsync(this CloudTable tbl, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginCreateIfNotExists(null, null);
            else
                result = tbl.BeginCreateIfNotExists(opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndCreateIfNotExists(ar);
            });
        }

        public static Task DeleteAsync(this CloudTable tbl, CancellationToken token)
        {
            return tbl.DeleteAsync(null, null, token);
        }

        public static Task DeleteAsync(this CloudTable tbl, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginDelete(null, null);
            else
                result = tbl.BeginDelete(opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                try
                {
                    tbl.EndDelete(ar);
                }
                catch (StorageException se)
                {
                    // Todo: Use the PnP azure retry block here for transient exceptions
                    throw;
                }
            });
        }

        public static Task<bool> DeleteIfNotExistsAsync(this CloudTable tbl, CancellationToken token)
        {
            return tbl.DeleteIfNotExistsAsync(null, null, token);
        }

        public static Task<bool> DeleteIfNotExistsAsync(this CloudTable tbl, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginDeleteIfExists(null, null);
            else
                result = tbl.BeginDeleteIfExists(opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndDeleteIfExists(ar);
            });
        }

        public static Task<bool> ExistsAsync(this CloudTable tbl, CancellationToken token)
        {
            return tbl.ExistsAsync(null, null, token);
        }

        public static Task<bool> ExistsAsync(this CloudTable tbl, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginExists(null, null);
            else
                result = tbl.BeginExists(opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndExists(ar);
            });
        }

        public static Task<TableResult> ExecuteAsync(this CloudTable tbl, TableOperation operation, CancellationToken token)
        {
            return tbl.ExecuteAsync(operation, null, null, token);
        }

        public static Task<TableResult> ExecuteAsync(this CloudTable tbl, TableOperation operation, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginExecute(operation, null, null);
            else
                result = tbl.BeginExecute(operation, opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndExecute(ar);
            });
        }

        public static Task<IList<TableResult>> ExecuteBatchAsync(this CloudTable tbl, TableBatchOperation operation, CancellationToken token)
        {
            return tbl.ExecuteBatchAsync(operation, null, null, token);
        }

        public static Task<IList<TableResult>> ExecuteBatchAsync(this CloudTable tbl, TableBatchOperation operation, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginExecuteBatch(operation, null, null);
            else
                result = tbl.BeginExecuteBatch(operation, opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndExecuteBatch(ar);
            });
        }

        // Overload #1
        public static Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(this CloudTable tbl, TableQuery query, TableContinuationToken continuationToken, CancellationToken token)
        {
            return tbl.ExecuteQuerySegmentedAsync(query, continuationToken, null, null, token);
        }

        // Overload #4
        public static Task<TableQuerySegment<DynamicTableEntity>> ExecuteQuerySegmentedAsync(this CloudTable tbl, TableQuery query, TableContinuationToken continuationToken, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginExecuteQuerySegmented(query, continuationToken, null, null);
            else
                result = tbl.BeginExecuteQuerySegmented(query, continuationToken, opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndExecuteQuerySegmented(ar);
            });
        }

        // Overload 2
        public static Task<TableQuerySegment<T>> ExecuteQuerySegmentedAsync<T>(this CloudTable tbl, TableQuery<T> query, TableContinuationToken continuationToken, CancellationToken token) where T : ITableEntity, new()
        {
            return tbl.ExecuteQuerySegmentedAsync<T>(query, continuationToken, null, null, token);
        }

        // Overload 5
        public static Task<TableQuerySegment<TElement>> ExecuteQuerySegmentedAsync<TElement>(this CloudTable tbl, TableQuery<TElement> query, TableContinuationToken continuationToken, TableRequestOptions opt, OperationContext ctx, CancellationToken token) where TElement : ITableEntity, new()
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginExecuteQuerySegmented<TElement>(query, continuationToken, null, null);
            else
                result = tbl.BeginExecuteQuerySegmented<TElement>(query, continuationToken, opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndExecuteQuerySegmented<TElement>(ar);
            });
        }

        // Overload 3
        public static Task<TableQuerySegment<R>> ExecuteQuerySegmentedAsync<T, R>(this CloudTable tbl, TableQuery<T> query, EntityResolver<R> resolver, TableContinuationToken continuationToken, CancellationToken token) where T : ITableEntity, new()
        {
            return tbl.ExecuteQuerySegmentedAsync2<T, R>(query, resolver, continuationToken, null, null, token);
        }

        // Overload 6
        public static Task<TableQuerySegment<R>> ExecuteQuerySegmentedAsync2<TElement, R>(this CloudTable tbl, TableQuery<TElement> query, EntityResolver<R> resolver, TableContinuationToken continuationToken, TableRequestOptions opt, OperationContext ctx, CancellationToken token) where TElement : ITableEntity, new()
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginExecuteQuerySegmented<TElement, R>(query, resolver, continuationToken, null, null);
            else
                result = tbl.BeginExecuteQuerySegmented<TElement, R>(query, resolver, continuationToken, opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                return tbl.EndExecuteQuerySegmented<R>(ar);
            });
        }

        public static Task<TablePermissions> GetPermissionsAsync(this CloudTable tbl, CancellationToken token)
        {
            return tbl.GetPermissionsAsync(null, null, token);
        }

        public static Task<TablePermissions> GetPermissionsAsync(this CloudTable tbl, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginGetPermissions(null, null);
            else
                result = tbl.BeginGetPermissions(opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                try
                {
                    return tbl.EndGetPermissions(ar);
                }
                catch (StorageException se)
                {
                    // Todo: Use the PnP azure retry block here for transient exceptions
                    throw;
                }
            });
        }

        public static Task SetPermissionsAsync(this CloudTable tbl, TablePermissions perms, CancellationToken token)
        {
            return tbl.SetPermissionsAsync(perms, null, null, token);
        }

        public static Task SetPermissionsAsync(this CloudTable tbl, TablePermissions perms, TableRequestOptions opt, OperationContext ctx, CancellationToken token)
        {
            ICancellableAsyncResult result = null;

            if (opt == null && ctx == null)
                result = tbl.BeginSetPermissions(perms, null, null);
            else
                result = tbl.BeginSetPermissions(perms, opt, ctx, null, null);

            var cancellationRegistration = token.Register(result.Cancel);

            return Task.Factory.FromAsync(result, ar =>
            {
                cancellationRegistration.Dispose();
                try
                {
                    tbl.EndSetPermissions(ar);
                }
                catch (StorageException se)
                {
                    // Todo: Use the PnP azure retry block here for transient exceptions
                    throw;
                }
            });
        }
    }
}