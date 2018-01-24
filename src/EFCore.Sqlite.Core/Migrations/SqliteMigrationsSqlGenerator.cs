// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Internal;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Migrations
{
    public class SqliteMigrationsSqlGenerator : MigrationsSqlGenerator
    {
        private readonly IMigrationsAnnotationProvider _migrationsAnnotations;

        private readonly Dictionary<string, List<RenameColumnOperation>> _tableRebuilds = new Dictionary<string, List<RenameColumnOperation>>();

        public SqliteMigrationsSqlGenerator(
            [NotNull] MigrationsSqlGeneratorDependencies dependencies,
            [NotNull] IMigrationsAnnotationProvider migrationsAnnotations)
            : base(dependencies)
            => _migrationsAnnotations = migrationsAnnotations;

        public override IReadOnlyList<MigrationCommand> Generate(IReadOnlyList<MigrationOperation> operations, IModel model = null)
            => base.Generate(WorkAroundSqliteLimitations(operations, model), model);

        private IReadOnlyList<MigrationOperation> WorkAroundSqliteLimitations(IReadOnlyList<MigrationOperation> migrationOperations, IModel model)
        {
            // There could be multiple calls issued to Generate(), so need to reset state on each call
            _tableRebuilds.Clear();

            var operations = new List<MigrationOperation>();
            foreach (var operation in migrationOperations)
            {
                switch (operation)
                {
                    case AddForeignKeyOperation foreignKeyOperation:
                        var table = migrationOperations
                            .OfType<CreateTableOperation>()
                            .FirstOrDefault(o => o.Name == foreignKeyOperation.Table);

                        // If corresponding CreateTableOperation is found move the foreign key creation inside, otherwise trigger rebuild of existing table
                        if (table != null)
                        {
                            table.ForeignKeys.Add(foreignKeyOperation);
                        }
                        else
                        {
                            RegisterTableForRebuild(foreignKeyOperation.Table);
                        }
                        break;
                    case AddPrimaryKeyOperation addPrimaryKeyOperation:
                        RegisterTableForRebuild(addPrimaryKeyOperation.Table);
                        break;
                    case AddUniqueConstraintOperation addUniqueConstraintOperation:
                        RegisterTableForRebuild(addUniqueConstraintOperation.Table);
                        break;
                    case DropColumnOperation dropColumnOperation:
                        RegisterTableForRebuild(dropColumnOperation.Table);
                        break;
                    case DropForeignKeyOperation dropForeignKeyOperation:
                        RegisterTableForRebuild(dropForeignKeyOperation.Table);
                        break;
                    case DropPrimaryKeyOperation dropPrimaryKeyOperation:
                        RegisterTableForRebuild(dropPrimaryKeyOperation.Table);
                        break;
                    case DropUniqueConstraintOperation dropUniqueConstraintOperation:
                        RegisterTableForRebuild(dropUniqueConstraintOperation.Table);
                        break;
                    case RenameColumnOperation renameColumnOperation:
                        RegisterTableForRebuild(renameColumnOperation.Table, renameColumnOperation);
                        break;
                    case RenameIndexOperation renameIndexOperation:
                        RegisterTableForRebuild(renameIndexOperation.Table);
                        break;
                    case AlterColumnOperation alterColumnOperation:
                        RegisterTableForRebuild(alterColumnOperation.Table);
                        break;
                    default:
                        operations.Add(operation);
                        break;
                }
            }

            operations.AddRange(GenerateTableRebuilds(model));

            return operations.AsReadOnly();
        }

        private void RegisterTableForRebuild(string tableName, RenameColumnOperation operation = null)
        {
            if (!_tableRebuilds.ContainsKey(tableName))
            {
                _tableRebuilds.Add(tableName, new List<RenameColumnOperation>());
            }

            if (operation != null)
            {
                _tableRebuilds[tableName].Add(operation);
            }
        }

        /// <summary>
        /// Table rebuild according to https://sqlite.org/lang_altertable.html
        /// </summary>
        private IEnumerable<MigrationOperation> GenerateTableRebuilds(IModel model)
        {
            var operations = new List<MigrationOperation>();
            foreach (var table in _tableRebuilds)
            {
                var modelDiffer = new MigrationsModelDiffer(Dependencies.TypeMapper, _migrationsAnnotations);

                var diffs = modelDiffer.GetDifferences(null, model);

                var createTableOperation = (CreateTableOperation)diffs.First(y =>
                    y.GetType() == typeof(CreateTableOperation) && ((CreateTableOperation)y).Name == table.Key);

                createTableOperation.Name = table.Key + "_new";

                var indexOperations = diffs.Where(y =>
                    y.GetType() == typeof(CreateIndexOperation) && ((CreateIndexOperation)y).Name.StartsWith($"IX_{table.Key}_")).ToList();

                operations.Add(new SqlOperation { Sql = "PRAGMA foreign_keys=OFF;", SuppressTransaction = true });

                operations.Add(createTableOperation);

                var insertColumns = createTableOperation.Columns.Select(y => y.Name);

                var selectColumns = new List<string>();

                foreach (var insertColumn in insertColumns)
                {
                    var renameColumnOperation = table.Value.FirstOrDefault(y => y.NewName == insertColumn);

                    if (renameColumnOperation != null)
                    {
                        selectColumns.Add(renameColumnOperation.Name);
                    }
                    else
                    {
                        selectColumns.Add(insertColumn);
                    }
                }

                operations.Add(new SqlOperation
                {
                    Sql = $"INSERT INTO {table.Key + "_new"} ({ColumnList(insertColumns.ToArray())}) " +
                          $"SELECT {ColumnList(selectColumns.ToArray())} FROM {table.Key}"
                });

                operations.Add(new DropTableOperation { Name = table.Key });

                operations.Add(new RenameTableOperation { Name = table.Key + "_new", NewName = table.Key });

                operations.AddRange(indexOperations);

                operations.Add(new SqlOperation { Sql = "PRAGMA foreign_keys=ON;", SuppressTransaction = true });
            }

            return operations;
        }

        protected override void Generate(DropIndexOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Check.NotNull(operation, nameof(operation));
            Check.NotNull(builder, nameof(builder));

            builder
                .Append("DROP INDEX ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator)
                .EndCommand();
        }

        protected override void Generate(RenameTableOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Check.NotNull(operation, nameof(operation));
            Check.NotNull(builder, nameof(builder));

            if (operation.NewName != null)
            {
                builder
                    .Append("ALTER TABLE ")
                    .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                    .Append(" RENAME TO ")
                    .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.NewName))
                    .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator)
                    .EndCommand();
            }
        }

        protected override void Generate(CreateTableOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            Check.NotNull(operation, nameof(operation));
            Check.NotNull(builder, nameof(builder));

            // Lifts a primary key definition into the typename.
            // This handles the quirks of creating integer primary keys using autoincrement, not default rowid behavior.
            if (operation.PrimaryKey?.Columns.Length == 1)
            {
                var columnOp = operation.Columns.FirstOrDefault(o => o.Name == operation.PrimaryKey.Columns[0]);
                if (columnOp != null)
                {
                    columnOp.AddAnnotation(SqliteAnnotationNames.InlinePrimaryKey, true);
                    if (!string.IsNullOrEmpty(operation.PrimaryKey.Name))
                    {
                        columnOp.AddAnnotation(SqliteAnnotationNames.InlinePrimaryKeyName, operation.PrimaryKey.Name);
                    }
                    operation.PrimaryKey = null;
                }
            }

            base.Generate(operation, model, builder);
        }

        protected override void ColumnDefinition(
            string schema,
            string table,
            string name,
            Type clrType,
            string type,
            bool? unicode,
            int? maxLength,
            bool rowVersion,
            bool nullable,
            object defaultValue,
            string defaultValueSql,
            string computedColumnSql,
            IAnnotatable annotatable,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            base.ColumnDefinition(
                schema, table, name, clrType, type, unicode, maxLength, rowVersion, nullable,
                defaultValue, defaultValueSql, computedColumnSql, annotatable, model, builder);

            var inlinePk = annotatable[SqliteAnnotationNames.InlinePrimaryKey] as bool?;
            if (inlinePk == true)
            {
                var inlinePkName = annotatable[
                    SqliteAnnotationNames.InlinePrimaryKeyName] as string;
                if (!string.IsNullOrEmpty(inlinePkName))
                {
                    builder
                        .Append(" CONSTRAINT ")
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(inlinePkName));
                }
                builder.Append(" PRIMARY KEY");
                var autoincrement = annotatable[SqliteAnnotationNames.Autoincrement] as bool?
                    // NB: Migrations scaffolded with version 1.0.0 don't have the prefix. See #6461
                    ?? annotatable[SqliteAnnotationNames.LegacyAutoincrement] as bool?;
                if (autoincrement == true)
                {
                    builder.Append(" AUTOINCREMENT");
                }
            }
        }

        #region Invalid migration operations

        // These operations can be accomplished instead with a table-rebuild
        protected override void Generate(AddForeignKeyOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(AddPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(AddUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(DropColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(DropForeignKeyOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(DropPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(DropUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(RenameColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(RenameIndexOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        protected override void Generate(AlterColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.InvalidMigrationOperation(operation.GetType().ShortDisplayName()));
        }

        #endregion

        #region Ignored schema operations

        protected override void Generate(EnsureSchemaOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
        }

        protected override void Generate(DropSchemaOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
        }

        #endregion

        #region Sequences not supported

        // SQLite does not have sequences
        protected override void Generate(RestartSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.SequencesNotSupported);
        }

        protected override void Generate(CreateSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.SequencesNotSupported);
        }

        protected override void Generate(RenameSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.SequencesNotSupported);
        }

        protected override void Generate(AlterSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.SequencesNotSupported);
        }

        protected override void Generate(DropSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException(SqliteStrings.SequencesNotSupported);
        }

        #endregion
    }
}
