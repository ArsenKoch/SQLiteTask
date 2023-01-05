package ua.cn.stu.tabs.model.boxes

import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.graphics.Color
import androidx.core.content.contentValuesOf
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.flow.*
import ua.cn.stu.tabs.model.AuthException
import ua.cn.stu.tabs.model.accounts.AccountsRepository
import ua.cn.stu.tabs.model.boxes.entities.Box
import ua.cn.stu.tabs.model.sqlite.AppSQLiteContract
import ua.cn.stu.tabs.model.sqlite.wrapSQLiteException

class SQLiteBoxesRepository(
    private val db: SQLiteDatabase,
    private val accountsRepository: AccountsRepository,
    private val ioDispatcher: CoroutineDispatcher
) : BoxesRepository {

    private val reconstructFlow = MutableSharedFlow<Unit>(replay = 1).also { it.tryEmit(Unit) }

    override suspend fun getBoxes(onlyActive: Boolean): Flow<List<Box>> {
        return combine(accountsRepository.getAccount(), reconstructFlow) { account, _ ->
            queryBoxes(onlyActive, account?.id)
        }.flowOn(ioDispatcher)
    }

    override suspend fun activateBox(box: Box) = wrapSQLiteException(ioDispatcher) {
        setActiveFlagForBox(box, true)
    }

    override suspend fun deactivateBox(box: Box) = wrapSQLiteException(ioDispatcher) {
        setActiveFlagForBox(box, false)
    }

    private suspend fun setActiveFlagForBox(box: Box, isActive: Boolean) {
        val account = accountsRepository.getAccount().first() ?: throw AuthException()
        saveActiveFlag(account.id, box.id, isActive)
        reconstructFlow.tryEmit(Unit)
    }

    private fun queryBoxes(onlyActive: Boolean, accountId: Long?): List<Box> {
        if (accountId == null) return emptyList()

        val cursor = queryBoxes(onlyActive, accountId)
        return cursor.use {
            val list = mutableListOf<Box>()
            while (cursor.moveToNext()) {
                list.add(parseBox(cursor))
            }
            return@use list
        }
    }

    private fun parseBox(cursor: Cursor): Box {
        return Box(
            id = cursor.getLong(cursor.getColumnIndexOrThrow(AppSQLiteContract.BoxesTable.COLUMN_ID)),
            colorName = cursor.getString(cursor.getColumnIndexOrThrow(AppSQLiteContract.BoxesTable.COLUMN_COLOR_NAME)),
            colorValue = Color.parseColor(
                cursor.getString(
                    cursor.getColumnIndexOrThrow(
                        AppSQLiteContract.BoxesTable.COLUMN_COLOR_VALUE
                    )
                )
            )
        )
    }

    private fun saveActiveFlag(accountId: Long, boxId: Long, isActive: Boolean) {
        db.insertWithOnConflict(
            AppSQLiteContract.AccountsBoxesSettingsTable.TABLE_NAME, null, contentValuesOf(
                AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_BOX_ID to boxId,
                AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_ACCOUNT_ID to accountId,
                AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_IS_ACTIVE to isActive,
            ), SQLiteDatabase.CONFLICT_REPLACE
        )
    }

    private fun queryBoxes(onlyActive: Boolean, accountId: Long): Cursor {
        return if (onlyActive) {
            val sql =   " SELECT ${AppSQLiteContract.BoxesTable.TABLE_NAME}.* " +
                        " FROM ${AppSQLiteContract.BoxesTable.TABLE_NAME}" +
                        " LEFT JOIN ${AppSQLiteContract.AccountsBoxesSettingsTable.TABLE_NAME}" +
                        " ON ${AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_BOX_ID} = ${AppSQLiteContract.BoxesTable.COLUMN_ID}" +
                        " AND ${AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_ACCOUNT_ID} = ?" +
                        " WHERE ${AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_IS_ACTIVE} IS NULL" +
                        " OR ${AppSQLiteContract.AccountsBoxesSettingsTable.COLUMN_IS_ACTIVE} = 1"
            db.rawQuery(sql, arrayOf(accountId.toString()))
        } else {
            db.rawQuery("SELECT * FROM ${AppSQLiteContract.BoxesTable.TABLE_NAME}", null)
        }
    }
}