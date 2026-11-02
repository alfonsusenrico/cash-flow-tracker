package com.alfonsusenrico.financialtracker.listener.ui.shell

import androidx.compose.animation.EnterTransition
import androidx.compose.animation.ExitTransition
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Snackbar
import androidx.compose.material3.SnackbarDuration
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.SnackbarResult
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Modifier
import androidx.compose.ui.semantics.clearAndSetSemantics
import androidx.navigation.NavGraph.Companion.findStartDestination
import androidx.navigation.NavHostController
import androidx.navigation.NavType
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import androidx.navigation.navArgument
import com.alfonsusenrico.financialtracker.listener.ui.activity.ActivityRoute
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureDetailRoute
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureSheetHost
import com.alfonsusenrico.financialtracker.listener.ui.capture.CaptureViewModel
import com.alfonsusenrico.financialtracker.listener.ui.detail.DetailRoute
import com.alfonsusenrico.financialtracker.listener.ui.home.HomeRoute
import com.alfonsusenrico.financialtracker.listener.ui.model.ActivityFilter
import com.alfonsusenrico.financialtracker.listener.ui.model.CapturePresentation
import com.alfonsusenrico.financialtracker.listener.ui.questions.QuestionsRoute
import com.alfonsusenrico.financialtracker.listener.ui.settings.SettingsRoute
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme
import com.alfonsusenrico.financialtracker.listener.ui.theme.rememberReducedMotion
import kotlinx.coroutines.launch

/** Shell-level actions every screen shares: messages, and delete with undo. */
class ShellActions(
    val message: (String) -> Unit,
    val deleteWithUndo: (Long) -> Unit
)

@Composable
fun CompanionApp(
    viewModel: CompanionViewModel,
    captureViewModel: CaptureViewModel,
    listenerPermissionGranted: Boolean,
    navController: NavHostController = rememberNavController()
) {
    val colors = CompanionTheme.colors
    val lists by viewModel.lists.collectAsState()
    val sheetOpen by captureViewModel.sheetOpen.collectAsState()
    val accounts by viewModel.accounts.collectAsState()
    val categories by viewModel.categories.collectAsState()
    val openRequest by viewModel.openRequest.collectAsState()
    val snackbar = remember { SnackbarHostState() }
    val scope = rememberCoroutineScope()
    val reducedMotion = rememberReducedMotion()
    val backStack by navController.currentBackStackEntryAsState()
    val route = backStack?.destination?.route
    val currentTab = Tab.entries.firstOrNull { route?.substringBefore('?') == it.route }

    val actions = remember(viewModel) {
        ShellActions(
            message = { text ->
                scope.launch {
                    snackbar.currentSnackbarData?.dismiss()
                    snackbar.showSnackbar(text, withDismissAction = true, duration = SnackbarDuration.Long)
                }
            },
            deleteWithUndo = { rowId ->
                viewModel.deleteFromPhone(rowId) { removed, previous ->
                    if (removed && previous != null) scope.launch {
                        snackbar.currentSnackbarData?.dismiss()
                        val result = snackbar.showSnackbar(
                            "Dihapus dari aplikasi", actionLabel = "Urungkan", withDismissAction = true,
                            duration = SnackbarDuration.Long
                        )
                        if (result == SnackbarResult.ActionPerformed) viewModel.undoDelete(previous)
                    }
                }
            }
        )
    }

    fun selectTab(tab: Tab) {
        navController.navigate(tab.route) {
            popUpTo(navController.graph.findStartDestination().id) { saveState = true }
            launchSingleTop = true
            restoreState = true
        }
    }

    // Tanya is a tab: a question opened from elsewhere becomes that tab's page instead of a page on top
    // of the tab it came from, so returning to that tab shows the tab itself.
    fun openQuestions(rowId: Long?) {
        navController.navigate(Routes.questions(rowId)) {
            popUpTo(navController.graph.findStartDestination().id) { saveState = true }
        }
    }

    fun openActivity(filter: ActivityFilter) {
        viewModel.setFilter(filter)
        selectTab(Tab.ACTIVITY)
    }

    fun openCapture(item: CapturePresentation) {
        if (item.question != null) captureViewModel.open(item.id) else navController.navigate(Routes.capture(item.id))
    }

    // A capture that changed the ledger makes the tracker's own summary stale; it is fetched again, never summed here.
    LaunchedEffect(captureViewModel) {
        captureViewModel.ledgerChanged.collect { viewModel.refreshLedgerSummary() }
    }

    LaunchedEffect(openRequest) {
        val request = openRequest ?: return@LaunchedEffect
        if (request.toQuestions) openQuestions(request.rowId) else navController.navigate(Routes.detail(request.rowId))
        viewModel.consumeOpenRequest()
    }

    Box(Modifier.fillMaxSize()) {
        Scaffold(
            // While the capture sheet is open the tab behind it is inert for TalkBack, as `inert` is in the prototype.
            modifier = if (sheetOpen) Modifier.clearAndSetSemantics {} else Modifier,
            containerColor = colors.bg,
            snackbarHost = {
                SnackbarHost(snackbar) { data ->
                    Snackbar(
                        data, containerColor = colors.snackbarBg, contentColor = colors.snackbarFg,
                        actionColor = colors.snackbarAction, dismissActionContentColor = colors.snackbarFg
                    )
                }
            },
            bottomBar = {
                if (currentTab != null) {
                    CompanionBottomBar(currentTab, lists.questions.size, onSelect = ::selectTab) {
                        CatatButton(open = sheetOpen, onClick = { captureViewModel.open() })
                    }
                }
            }
        ) { padding ->
            NavHost(
                navController = navController,
                startDestination = Routes.HOME,
                modifier = Modifier.fillMaxSize().background(colors.bg).padding(padding),
                enterTransition = { if (reducedMotion) EnterTransition.None else fadeIn(tween(150)) },
                exitTransition = { if (reducedMotion) ExitTransition.None else fadeOut(tween(150)) },
                popEnterTransition = { if (reducedMotion) EnterTransition.None else fadeIn(tween(150)) },
                popExitTransition = { if (reducedMotion) ExitTransition.None else fadeOut(tween(150)) }
            ) {
                composable(Routes.HOME) {
                    HomeRoute(
                        viewModel = viewModel,
                        listenerPermissionGranted = listenerPermissionGranted,
                        onOpenQuestions = ::openQuestions,
                        onOpenActivity = ::openActivity,
                        onOpenSettings = { selectTab(Tab.SETTINGS) },
                        captureViewModel = captureViewModel
                    )
                }
                composable(Routes.ACTIVITY) {
                    ActivityRoute(
                        viewModel = viewModel,
                        captureViewModel = captureViewModel,
                        actions = actions,
                        onOpenRow = { row, toQuestions -> if (toQuestions) openQuestions(row) else navController.navigate(Routes.detail(row)) },
                        onOpenCapture = ::openCapture
                    )
                }
                composable(
                    "${Routes.QUESTIONS}?${Routes.ROW}={${Routes.ROW}}",
                    arguments = listOf(navArgument(Routes.ROW) { type = NavType.LongType; defaultValue = -1L })
                ) { entry ->
                    QuestionsRoute(
                        viewModel = viewModel,
                        actions = actions,
                        requestedRowId = entry.arguments?.getLong(Routes.ROW)?.takeIf { it >= 0 },
                        onOpenDetail = { navController.navigate(Routes.detail(it)) },
                        onHome = { selectTab(Tab.HOME) }
                    )
                }
                composable(Routes.SETTINGS) {
                    SettingsRoute(viewModel = viewModel, actions = actions)
                }
                composable(
                    "${Routes.DETAIL}/{${Routes.ROW}}",
                    arguments = listOf(navArgument(Routes.ROW) { type = NavType.LongType })
                ) { entry ->
                    DetailRoute(
                        viewModel = viewModel,
                        actions = actions,
                        rowId = entry.arguments!!.getLong(Routes.ROW),
                        onBack = { navController.popBackStack() },
                        onOpenQuestion = ::openQuestions
                    )
                }
                composable(
                    "${Routes.CAPTURE}/{${Routes.ROW}}",
                    arguments = listOf(navArgument(Routes.ROW) { type = NavType.LongType })
                ) { entry ->
                    CaptureDetailRoute(
                        viewModel = captureViewModel,
                        accounts = accounts,
                        categories = categories,
                        actions = actions,
                        captureId = entry.arguments!!.getLong(Routes.ROW),
                        onBack = { navController.popBackStack() }
                    )
                }
            }
        }

        if (sheetOpen) {
            CaptureSheetHost(
                viewModel = captureViewModel,
                accounts = accounts,
                categories = categories,
                onOpenQueue = { openActivity(ActivityFilter.LOCAL) },
                onOpenDetail = { navController.navigate(Routes.capture(it)) }
            )
        }
    }
}
